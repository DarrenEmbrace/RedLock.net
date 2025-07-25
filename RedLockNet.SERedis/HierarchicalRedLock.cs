using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;
using RedLockNet.SERedis.Configuration;
using RedLockNet.SERedis.Internal;
using RedLockNet.SERedis.Util;
using StackExchange.Redis;

namespace RedLockNet.SERedis
{
    public class HierarchicalRedLock : RedLock
    {
        private static readonly string LockWithFileCheckScript = EmbeddedResourceLoader.GetEmbeddedResource("RedLockNet.SERedis.Lua.LockHierarchical.lua");
        public string BlockingResource { get; set; } // if it is acquired elsewhere
        public string ParentResource { get; }

        protected HierarchicalRedLock(ILogger<RedLock> logger, ICollection<RedisConnection> redisCaches, string parentResource, string resource, TimeSpan expiryTime, string LockInfo, TimeSpan? waitTime = null, TimeSpan? retryTime = null, RedLockRetryConfiguration retryConfiguration = null, CancellationToken? cancellationToken = null) : base(logger, redisCaches, resource, expiryTime, LockInfo, waitTime, retryTime, retryConfiguration, cancellationToken)
        {
            this.ParentResource = parentResource;
        }
        
        protected override RedLockInstanceResult LockInstance(RedisConnection cache)
        {
            var redisKey = GetRedisKey(cache.RedisKeyFormat, Resource);
            var redisParentKey = GetRedisKey(cache.RedisKeyFormat, ParentResource);
            var host = GetHost(cache.ConnectionMultiplexer);
    
            RedLockInstanceResult result;
    
            try
            {
                logger.LogTrace($"LockInstance enter {host}: {redisKey}, {LockId}, {expiryTime}");
                
                // Attempt to acquire the lock using a Lua script that checks for a blocking resource
                var redisResult = cache.ConnectionMultiplexer
                    .GetDatabase(cache.RedisDatabase)
                    .ScriptEvaluate(
                        LockWithFileCheckScript,
                        new RedisKey[]
                        {
                            redisParentKey, redisKey
                        }, // KEYS[1] = parent lock key (only check), KEYS[2] = actual lock key (try to acquire)
                        new RedisValue[] {LockId, (int) expiryTime.TotalMilliseconds},
                        flags: CommandFlags.DemandMaster);

                // If the result is not null, it means there is a blocking resource
                // Could be from the parent (KEYS[1]) or from a failed attempt to acquire KEYS[2]
                // The script returns the *value* of the blocking key so we can identify the blocker
                if (!redisResult.IsNull)
                {
                    BlockingResource = (string) redisResult; // Save the value of the blocking key
                    return RedLockInstanceResult.Conflicted; // Indicate lock conflict due to existing blocker
                }

                return RedLockInstanceResult.Success;
            }
            catch (Exception ex)
            {
                logger.LogDebug($"Error locking lock instance {host}: {ex.Message}");
    
                result = RedLockInstanceResult.Error;
            }
    
            logger.LogTrace($"LockInstance exit {host}: {redisKey}, {LockId}, {result}");
    
            return result;
        }
        
        internal static HierarchicalRedLock CreateHierarchical(
            ILogger<RedLock> logger,
            ICollection<RedisConnection> redisCaches,
            string parentResource,
            string resource,
            TimeSpan expiryTime,
            string LockInfo,
            TimeSpan? waitTime = null,
            TimeSpan? retryTime = null,
            RedLockRetryConfiguration retryConfiguration = null,
            CancellationToken? cancellationToken = null)
        {
            var redisLock = new HierarchicalRedLock(
                logger,
                redisCaches,
                parentResource,
                resource,
                expiryTime,
                LockInfo,
                waitTime,
                retryTime,
                retryConfiguration,
                cancellationToken);
    
            redisLock.Start();
    			
            return redisLock;
        }

    }
}