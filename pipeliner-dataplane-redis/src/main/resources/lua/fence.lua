-- KEYS[1]=pg:{r}:fence; ARGV[1]=epoch. Sets the fence if stored <= epoch (1), else 0.
local cur = redis.call('GET', KEYS[1])
if cur and tonumber(cur) > tonumber(ARGV[1]) then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'PX', 604800000)
return 1
