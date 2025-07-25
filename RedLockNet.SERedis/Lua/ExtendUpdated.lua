-- Get the actual lock key
local key = KEYS[1]

-- Compute the corresponding release marker key (used to indicate this lock should be released)
local releaseKey = key .. "_release"

-- Check if the release key exists and matches our LockId (ARGV[1])
local releaseVal = redis.call('get', releaseKey)
if (releaseVal == ARGV[1]) then
    -- We own the lock, but it's been flagged for release (by us or externally)
    -- In this case, we should not extend the lock
    return -1
end

-- If the release key exists but doesn't match our LockId, someone else owns the lock
-- We'll proceed to extend it (handled by the following code)
local currentVal = redis.call('get', key)
if (currentVal == false) then
    return redis.call('set', key, ARGV[1], 'PX', ARGV[2]) and 1 or 0
elseif (currentVal == ARGV[1]) then
    return redis.call('pexpire', key, ARGV[2])
else
    return -1
end
