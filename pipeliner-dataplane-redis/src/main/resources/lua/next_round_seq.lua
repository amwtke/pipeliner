-- KEYS[1]=pg:roundseq; ARGV[1]=floor. Raises the counter to at least floor, then INCR: the new sequence is always > floor.
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
if cur < tonumber(ARGV[1]) then redis.call('SET', KEYS[1], ARGV[1]) end
return redis.call('INCR', KEYS[1])
