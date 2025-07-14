using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RedLockNet.SERedis.Configuration;
using RedLockNet.SERedis.Internal;

namespace RedLockNet.SERedis
{
    /*
     * This shouldn't really exist, but we have a use case for removing locks that we do not own
     */
    public class ReleaseableRedlockFactory : HierarchicalLockFactory
    {
        public ReleaseableRedlockFactory(RedLockConfiguration configuration) : base(configuration)
        {
            
        }

        public override IRedLock CreateLock(string resource, TimeSpan expiryTime,string lockInfo = null)
        {
            return ReleaseableRedlock.Create(
                this.loggerFactory.CreateLogger<RedLock>(),
                redisCaches,
                resource,
                expiryTime,
                lockInfo,
                retryConfiguration: configuration.RetryConfiguration);
        }

        public override IRedLock CreateLock(string resource, TimeSpan expiryTime, TimeSpan waitTime, TimeSpan retryTime,
            CancellationToken? cancellationToken = null, string lockInfo = null)
        {
            return ReleaseableRedlock.Create(
                this.loggerFactory.CreateLogger<RedLock>(),
                redisCaches,
                resource,
                expiryTime,
                lockInfo,
                waitTime,
                retryTime,
                configuration.RetryConfiguration,
                cancellationToken ?? CancellationToken.None);
        }

        public override IRedLock CreateHierarchicalLock(string parentResource, string resource, TimeSpan expiryTime, string lockInfo = null)
        {
            ILogger<RedLock> logger = this.loggerFactory.CreateLogger<RedLock>();
            ICollection<RedisConnection> redisCaches = this.redisCaches;
            string resource1 = resource;
            TimeSpan expiryTime1 = expiryTime;
            string LockInfo = lockInfo;
            RedLockRetryConfiguration retryConfiguration1 = this.configuration.RetryConfiguration;
            TimeSpan? waitTime = new TimeSpan?();
            TimeSpan? retryTime = new TimeSpan?();
            RedLockRetryConfiguration retryConfiguration2 = retryConfiguration1;
            CancellationToken? cancellationToken = new CancellationToken?();
            return ReleaseableHierarchicalRedlock.CreateHierarchical(logger, redisCaches, parentResource, resource1, expiryTime1, LockInfo, waitTime, retryTime, retryConfiguration2, cancellationToken);
        }

        public override IRedLock CreateHierarchicalLock(string parentResource, string resource, TimeSpan expiryTime, TimeSpan waitTime,
            TimeSpan retryTime, CancellationToken? cancellationToken = null, string lockInfo = null)
        {
            return ReleaseableHierarchicalRedlock.CreateHierarchical(this.loggerFactory.CreateLogger<RedLock>(), this.redisCaches, parentResource, resource, expiryTime, lockInfo, new TimeSpan?(waitTime), new TimeSpan?(retryTime), this.configuration.RetryConfiguration, new CancellationToken?(cancellationToken ?? CancellationToken.None));
        }

        public new static ReleaseableRedlockFactory Create(
            IList<RedLockMultiplexer> existingMultiplexers,
            ILoggerFactory loggerFactory = null)
        {
            return ReleaseableRedlockFactory.Create(existingMultiplexers, (RedLockRetryConfiguration) null, loggerFactory);
        }
        public new static ReleaseableRedlockFactory Create(
            IList<RedLockMultiplexer> existingMultiplexers,
            RedLockRetryConfiguration retryConfiguration,
            ILoggerFactory loggerFactory = null)
        {
            return new ReleaseableRedlockFactory(new RedLockConfiguration((RedLockConnectionProvider) new ExistingMultiplexersRedLockConnectionProvider()
            {
                Multiplexers = existingMultiplexers
            }, loggerFactory)
            {
                RetryConfiguration = retryConfiguration
            });
        }
        
        public async Task<RedRelease> ReleaseLockAsync(string resource)
        {
            return await RedRelease.ReleaseAsync(
                this.loggerFactory.CreateLogger<RedRelease>(),
                redisCaches,
                resource,
                configuration.RetryConfiguration).ConfigureAwait(false);
        
        }
    }
}