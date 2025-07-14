using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;
using RedLockNet.SERedis.Configuration;
using RedLockNet.SERedis.Internal;

namespace RedLockNet.SERedis
{
    public class HierarchicalLockFactory : RedLockFactory
    {
        public HierarchicalLockFactory(RedLockConfiguration configuration): base(configuration)
        {
        }
        
        public new static HierarchicalLockFactory Create(
            IList<RedLockMultiplexer> existingMultiplexers,
            ILoggerFactory loggerFactory = null)
        {
            return HierarchicalLockFactory.Create(existingMultiplexers, (RedLockRetryConfiguration) null, loggerFactory);
        }
        public new static HierarchicalLockFactory Create(
            IList<RedLockMultiplexer> existingMultiplexers,
            RedLockRetryConfiguration retryConfiguration,
            ILoggerFactory loggerFactory = null)
        {
            return new HierarchicalLockFactory(new RedLockConfiguration((RedLockConnectionProvider) new ExistingMultiplexersRedLockConnectionProvider()
            {
                Multiplexers = existingMultiplexers
            }, loggerFactory)
            {
                RetryConfiguration = retryConfiguration
            });
        }
        
        public virtual IRedLock CreateHierarchicalLock(string parentResource, string resource, TimeSpan expiryTime, string lockInfo = null)
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
            return HierarchicalRedLock.CreateHierarchical(logger, redisCaches, parentResource, resource1, expiryTime1, LockInfo, waitTime, retryTime, retryConfiguration2, cancellationToken);
        }
        
        public virtual IRedLock CreateHierarchicalLock(
            string parentResource,
            string resource,
            TimeSpan expiryTime,
            TimeSpan waitTime,
            TimeSpan retryTime,
            CancellationToken? cancellationToken = null,
            string lockInfo = null)
        {
            return HierarchicalRedLock.CreateHierarchical(this.loggerFactory.CreateLogger<RedLock>(), this.redisCaches, parentResource, resource, expiryTime, lockInfo, new TimeSpan?(waitTime), new TimeSpan?(retryTime), this.configuration.RetryConfiguration, new CancellationToken?(cancellationToken ?? CancellationToken.None));
        }
    }
}