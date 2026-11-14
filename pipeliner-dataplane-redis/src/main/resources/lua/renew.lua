-- KEYS[1]=stream; ARGV[1]=group, ARGV[2]=consumer, ARGV[3]=entry id.
-- Resets the idle clock (XCLAIM min-idle 0) only if the entry is still pending FOR THIS consumer: returns 1 renewed, 0 not pending or owned by another consumer.
-- A missing stream/group raises NOGROUP like XPENDING does.
local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if #p == 0 or p[1][2] ~= ARGV[2] then return 0 end
local r = redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, ARGV[3], 'JUSTID')
return #r
