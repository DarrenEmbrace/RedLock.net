-- KEYS[1] = source key (redisKey)
-- KEYS[2] = destination key (releaseKey)

-- If the source key is missing, mimic "NotFound": return false, no error.
if redis.call('EXISTS', KEYS[1]) == 0 then
    return false
end

-- Always rename (overwrite) just like When.Always.
redis.call('RENAME', KEYS[1], KEYS[2])

-- Success
return true