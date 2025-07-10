-- Step 1: Check if KEYS[1] exists
local val = redis.call('GET', KEYS[1])
if val then
    return val
end

-- Step 2: Try to set KEYS[2] with NX and PX
local success = redis.call('SET', KEYS[2], ARGV[1], 'NX', 'PX', ARGV[2])
if not success then
    return redis.call('GET', KEYS[2])
end

-- Success (KEYS[1] did not exist, KEYS[2] was set)
return nil
