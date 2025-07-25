--[[
KEYS[1] is the parent lock key. We *do not* want to lock this key,
only check if it already exists (i.e., if a blocking resource is active).
KEYS[2] is the actual lock key we want to acquire.

We return the value of the key (if found) instead of a generic success/failure
so that the caller can determine what resource is causing the block.
]]

-- Step 1: Check if the parent lock key exists (i.e., blocking resource is active)
local val = redis.call('GET', KEYS[1])
if val then
    return val  -- Return blocking value to caller
end

-- Step 2: Try to acquire the actual lock using NX (set if not exists) with expiration (PX)
local success = redis.call('SET', KEYS[2], ARGV[1], 'NX', 'PX', ARGV[2])
if not success then
    -- Lock acquisition failed; return current value of the lock to identify who holds it
    return redis.call('GET', KEYS[2])
end

-- Lock acquired successfully, no blocking resource
return nil
