-- fence: KEYS[1]=pg:{r}:fence, ARGV[1]=master epoch; a higher stored epoch means this caller is a deposed master
local __f = redis.call('GET', KEYS[1])
if __f and tonumber(__f) > tonumber(ARGV[1]) then return -2 end
-- ARGV[2]=redis command, KEYS[2..]=its keys, ARGV[3..]=its remaining args
local cmd = {ARGV[2]}
for i = 2, #KEYS do cmd[#cmd + 1] = KEYS[i] end
for i = 3, #ARGV do cmd[#cmd + 1] = ARGV[i] end
local r = redis.call(unpack(cmd))
-- commands that create a key register it in the round's key registry
if ARGV[2] == 'HSET' or ARGV[2] == 'SET' then
  redis.call('SADD', (string.gsub(KEYS[1], ':fence$', ':keys')), KEYS[2])
end
if type(r) == 'table' then return 1 end
return r
