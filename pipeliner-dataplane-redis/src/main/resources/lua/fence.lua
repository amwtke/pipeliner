-- KEYS[1]=pg:{r}:fence; ARGV[1]=epoch. Sets the fence if stored <= epoch (1), else 0.
-- The fence has NO TTL: it must only ever increase while the round exists. purgeRoundData deletes it with the round's other keys.
local cur = redis.call('GET', KEYS[1])
if cur and tonumber(cur) > tonumber(ARGV[1]) then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
local __reg = string.gsub(KEYS[1], ':fence$', ':keys')
redis.call('SADD', __reg, KEYS[1])
return 1
