using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RedLockNet.SERedis.Configuration;
using RedLockNet.SERedis.Internal;
using RedLockNet.SERedis.Util;
using StackExchange.Redis;

namespace RedLockNet.SERedis
{
    public class ReleaseableRedlock : HierarchicalRedLock
    {
        

        private static readonly string ExtendIfNoReleaseScript =
            EmbeddedResourceLoader.GetEmbeddedResource("RedLockNet.SERedis.Lua.ExtendUpdated.lua");
        protected ReleaseableRedlock(ILogger<RedLock> logger, ICollection<RedisConnection> redisCaches,
            string parentResource, string resource, TimeSpan expiryTime, string LockInfo, TimeSpan? waitTime = null,
            TimeSpan? retryTime = null, RedLockRetryConfiguration retryConfiguration = null,
            CancellationToken? cancellationToken = null) : base(logger, redisCaches, parentResource, resource,
            expiryTime, LockInfo, waitTime, retryTime, retryConfiguration, cancellationToken)
        {
        }

        protected override string GetExtendScript()
        {
            return ExtendIfNoReleaseScript;
        }
    }

    public class RedRelease
    {
        protected readonly ICollection<RedisConnection> redisCaches;
        protected readonly ILogger<RedRelease> logger;
        protected readonly int quorum;
        protected readonly int quorumRetryCount;
        protected readonly int quorumRetryDelayMs;
        private bool isDisposed;
        private Timer lockKeepaliveTimer;
        public string Resource { get; }
        public RedReleaseStatus Status { get; private set; }
        public RedReleaseInstanceSummary InstanceSummary { get; private set; }
        
        protected CancellationToken cancellationToken;
        private readonly TimeSpan? waitTime;
        private readonly TimeSpan? retryTime;
        protected readonly TimeSpan expiryTime;
        private static readonly TimeSpan MinimumExpiryTime = TimeSpan.FromMilliseconds(10);
        private static readonly TimeSpan MinimumRetryTime = TimeSpan.FromMilliseconds(10);

        private const double ClockDriftFactor = 0.01;
        private static readonly long ClockPrecisionPaddingTicks = TimeSpan.FromMilliseconds(2).Ticks;
        
        private const int DefaultQuorumRetryCount = 3;
        private const int DefaultQuorumRetryDelayMs = 400;
        
        protected RedRelease(
            ILogger<RedRelease> logger,
            ICollection<RedisConnection> redisCaches,
            string resource,
            RedLockRetryConfiguration retryConfiguration = null)
        {
            this.logger = logger;

            this.redisCaches = redisCaches;

            quorum = redisCaches.Count / 2 + 1;
            quorumRetryCount = retryConfiguration?.RetryCount ?? DefaultQuorumRetryCount;
            quorumRetryDelayMs = retryConfiguration?.RetryDelayMs ?? DefaultQuorumRetryDelayMs;

            Resource = resource;
            this.expiryTime = MinimumExpiryTime;
            this.retryTime = MinimumRetryTime;
        }
        
        internal static async Task<RedRelease> ReleaseAsync(
            ILogger<RedRelease> logger,
            ICollection<RedisConnection> redisCaches,
            string resource,
            RedLockRetryConfiguration retryConfiguration = null)
        {
            var redisLock = new RedRelease(logger,
                redisCaches,
                resource,
                retryConfiguration);

            await redisLock.ReleaseAsyncThisIsTheOne().ConfigureAwait(false);

            return redisLock;
        }

        protected long GetRemainingValidityTicks(Stopwatch sw)
        {
            // Add 2 milliseconds to the drift to account for Redis expires precision,
            // which is 1 milliescond, plus 1 millisecond min drift for small TTLs.
            var driftTicks = (long) (expiryTime.Ticks * ClockDriftFactor) + ClockPrecisionPaddingTicks;
            var validityTicks = expiryTime.Ticks - sw.Elapsed.Ticks - driftTicks;
            return validityTicks;
        }
        
        protected RedReleaseStatus GetFailedReleaseStatus(RedReleaseInstanceSummary lockResult)
        {
            if (lockResult.Released >= quorum)
            {
                // if we got here with a quorum then validity must have expired
                return RedReleaseStatus.Expired;
            }

            if (lockResult.Released + lockResult.Conflicted >= quorum)
            {
                // we had enough instances for a quorum, but some were locked with another LockId
                return RedReleaseStatus.Conflicted;
            }

            return RedReleaseStatus.NoQuorum;
        }
        
        protected static RedReleaseInstanceSummary PopulateReleaseResult(
            IEnumerable<RedLockInstanceResult> instanceResults)
        {
            var acquired = 0;
            var conflicted = 0;
            var error = 0;

            foreach (var instanceResult in instanceResults)
            {
                switch (instanceResult)
                {
                    case RedLockInstanceResult.Success:
                        acquired++;
                        break;
                    case RedLockInstanceResult.Conflicted:
                        conflicted++;
                        break;
                    case RedLockInstanceResult.Error:
                        error++;
                        break;
                }
            }

            return new RedReleaseInstanceSummary(acquired, conflicted, error);
        }

        private async Task<RedReleaseInstanceSummary> FlagReleaseAsync()
        {
            var releaseTasks = redisCaches.Select(ReleaseFlagAsync);

            var lockResults = await TaskUtils.WhenAll(releaseTasks).ConfigureAwait(false);

            return PopulateReleaseResult(lockResults);
        }

        private async Task RemoveFlagReleaseAsync()
        {
            //We can introduce the concept of a semaphore here, but it will have to be controlled by the locking process itself?
            //await releaseSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                var unflagTasks = redisCaches.Select(ReleaseUnFlagAsync);
                await TaskUtils.WhenAll(unflagTasks).ConfigureAwait(false);
            }
            finally
            {
                //releaseSemaphore.Release();
            }
        }

        /*
         * This completely stuffs up the Redlock algorithm, but we have use cases where locks should be removed
         *
         * Basically what I think we need to do:
         * Change the existing lock key -> lock_key"release"
         * This should allow other locks to acquire a lock on the original key.
         * In the extend script, if there is a release keywork in the lock, then we delete the release key now, and the extend should fail
         * and the lock should dispose of itself or something.
         */
        private async Task<(RedReleaseStatus, RedReleaseInstanceSummary)> ReleaseAsyncThisIsTheOne()
        {
            /*
             * We treat the summary the same, RedReleaseInstanceSummary.acquired == successful flag
             */
            var flagSummary = new RedReleaseInstanceSummary();
            for (var i = 0; i < quorumRetryCount; i++)
            {
                var iteration = i + 1;
                logger.LogDebug($"Release attempt {iteration}/{quorumRetryCount}: {Resource}");

                var stopwatch = Stopwatch.StartNew();

                flagSummary = await FlagReleaseAsync().ConfigureAwait(false);

                var validityTicks = GetRemainingValidityTicks(stopwatch);

                logger.LogDebug(
                    $"Flagged {Resource} for release in {flagSummary.Released}/{redisCaches.Count} instances, quorum: {quorum}, validityTicks: {validityTicks}");

                if (flagSummary.Released >= quorum && validityTicks > 0)
                {
                    return (RedReleaseStatus.Released, flagSummary);
                }

                // we failed to flag enough locks for a quorum, unflag everything and try again
                await RemoveFlagReleaseAsync().ConfigureAwait(false);

                // only sleep if we have more retries left
                if (i < quorumRetryCount - 1)
                {
                    var sleepMs = ThreadSafeRandom.Next(quorumRetryDelayMs);

                    logger.LogDebug($"Sleeping {sleepMs}ms");

                    await TaskUtils.Delay(sleepMs, cancellationToken).ConfigureAwait(false);
                }
            }

            var status = GetFailedReleaseStatus(flagSummary);

            // give up
            logger.LogDebug(
                $"Could not acquire quorum after {quorumRetryCount} attempts, giving up: {Resource}. {flagSummary}.");

            return (status, flagSummary);
        }

        /*
         * At the moment all this does it rename the key we're trying to release by appending "release" to the key
         * This should "release" the lock once a quorum occurs.
         * The locks auto extender should now account for the release key and dispose of itself.
         */
        private async Task<RedLockInstanceResult> ReleaseFlagAsync(RedisConnection cache)
        {
            var redisKey = GetRedisKey(cache.RedisKeyFormat, Resource);
            var releaseKey = GetReleaseKey(cache.RedisKeyFormat, Resource);
            var host = GetHost(cache.ConnectionMultiplexer);

            RedLockInstanceResult result;

            try
            {
                logger.LogTrace($"ReleaseFlagAsync enter {host}: {redisKey}");
                bool renamed = await cache.ConnectionMultiplexer
                    .GetDatabase(cache.RedisDatabase)
                    .KeyRenameAsync(redisKey, releaseKey, When.Always, CommandFlags.DemandMaster)
                    .ConfigureAwait(false);

                result = renamed
                    ? RedLockInstanceResult.Success
                    : RedLockInstanceResult.Conflicted;

            }
            catch (Exception ex)
            {
                logger.LogDebug($"Error flagging lock release, instance {host}: {ex.Message}");

                result = RedLockInstanceResult.Error;
            }

            logger.LogTrace($"ReleaseFlagAsync exit {host}: {redisKey}, {result}");

            return result;
        }
        
        protected static string GetRedisKey(string redisKeyFormat, string resource)
        {
            return string.Format(redisKeyFormat, resource);
        }

        protected static string GetReleaseKey(string redisKeyFormat, string resource)
        {
            return GetRedisKey(redisKeyFormat, resource) + "_release";
        }
        
        protected static string GetHost(IConnectionMultiplexer cache)
        {
            var result = new StringBuilder();

            foreach (var endPoint in cache.GetEndPoints())
            {
                var server = cache.GetServer(endPoint);

                result.Append(server.EndPoint.GetFriendlyName());
                result.Append(" (");
                result.Append(server.IsSlave ? "slave" : "master");
                result.Append(server.IsConnected ? "" : ", disconnected");
                result.Append("), ");
            }

            if (result.Length >= 2)
            {
                result.Remove(result.Length - 2, 2);
            }

            return result.ToString();
        }

        /*
         * This method seems kind of good to me
         */

        private async Task<bool> ReleaseUnFlagAsync(RedisConnection cache)
        {
            var redisKey = GetRedisKey(cache.RedisKeyFormat, Resource);
            var releaseKey = GetReleaseKey(cache.RedisKeyFormat, Resource);
            var host = GetHost(cache.ConnectionMultiplexer);

            var redisResult = false;
            try
            {
                logger.LogTrace($"ReleaseFlagAsync enter {host}: {redisKey}");
                // Returns 1 on success, 0 on failure setting expiry or key not existing, -1 if the key value didn't match
                redisResult = (bool) await cache.ConnectionMultiplexer
                    .GetDatabase(cache.RedisDatabase)
                    .KeyRenameAsync(releaseKey, redisKey, When.Always, CommandFlags.DemandMaster)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogDebug($"Error flagging lock release, instance {host}: {ex.Message}");
            }

            logger.LogTrace($"ReleaseFlagAsync exit {host}: {redisKey}, {redisResult}");
            return redisResult;
        }
    }
    
    public enum RedReleaseStatus
    {
        /// <summary>
        /// The lock has not yet been successfully acquired, or has been released.
        /// </summary>
        Released,

        /// <summary>
        /// The lock was not acquired because there was no quorum available.
        /// </summary>
        NoQuorum,

        /// <summary>
        /// The lock was not acquired because it is currently locked with a different LockId.
        /// </summary>
        Conflicted,
        
        /// <summary>
        /// Adding this here as another stage, not sure what it should be called though
        /// </summary>
        Expired,
        
    }
    
    public struct RedReleaseInstanceSummary
    {
        public RedReleaseInstanceSummary(int released, int conflicted, int error)
        {
            this.Released = released;
            this.Conflicted = conflicted;
            this.Error = error;
        }

        public readonly int Released;
        public readonly int Conflicted;
        public readonly int Error;

        public override string ToString()
        {
            return $"Released: {Released}, Conflicted: {Conflicted}, Error: {Error}";
        }
    }
}