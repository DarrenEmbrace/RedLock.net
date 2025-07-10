using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RedLockNet.SERedis.Configuration;

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