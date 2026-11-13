-- fence: KEYS[1]=pg:{r}:fence, ARGV[1]=master epoch; a higher stored epoch means this caller is a deposed master
local __f = redis.call('GET', KEYS[1])
if __f and tonumber(__f) > tonumber(ARGV[1]) then return -2 end
-- KEYS[2]=stream; ARGV[2]=group
redis.pcall('XGROUP', 'CREATE', KEYS[2], ARGV[2], '0', 'MKSTREAM')
return 1
