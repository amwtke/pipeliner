-- fence: KEYS[1]=pg:{r}:fence, ARGV[1]=master epoch; a higher stored epoch means this caller is a deposed master
local __f = redis.call('GET', KEYS[1])
if __f and tonumber(__f) > tonumber(ARGV[1]) then return -2 end
-- KEYS[2]=split index set, KEYS[3]=split queue; ARGV[2]=split id, ARGV[3]=split json
if redis.call('SADD', KEYS[2], ARGV[2]) == 1 then
  redis.call('XADD', KEYS[3], '*', 's', ARGV[3])
  return 1
end
return 0
