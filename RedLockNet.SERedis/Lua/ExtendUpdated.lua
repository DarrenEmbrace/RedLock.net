local key = KEYS[1]
local releaseKey = key .. "_release"

local releaseVal = redis.call('get', releaseKey)
if (releaseVal == ARGV[1]) then
    return -1 -- Lock flagged for release; can't extend
end

local currentVal = redis.call('get', key)
if (currentVal == false) then
    return redis.call('set', key, ARGV[1], 'PX', ARGV[2]) and 1 or 0
elseif (currentVal == ARGV[1]) then
    return redis.call('pexpire', key, ARGV[2])
else
    return -1
end
