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
    				
                var redisResult = cache.ConnectionMultiplexer
                    .GetDatabase(cache.RedisDatabase)
                    .ScriptEvaluate(
                        LockWithFileCheckScript,
                        new RedisKey[] { redisParentKey, redisKey },
                        new RedisValue[] { LockId, (int)expiryTime.TotalMilliseconds },
                        flags: CommandFlags.DemandMaster);

                if (!redisResult.IsNull)
                {
                    BlockingResource = (string) redisResult;
                    return RedLockInstanceResult.Conflicted;
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