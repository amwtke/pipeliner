-- fence: KEYS[1]=pg:{r}:fence, ARGV[1]=master epoch; a higher stored epoch means this caller is a deposed master
local __f = redis.call('GET', KEYS[1])
if __f and tonumber(__f) > tonumber(ARGV[1]) then return -2 end
-- KEYS[2]=dlq; returns its length before deletion
local n = redis.call('XLEN', KEYS[2])
redis.call('DEL', KEYS[2])
return n
