-- fence: KEYS[1]=pg:{r}:fence, ARGV[1]=master epoch; a higher stored epoch means this caller is a deposed master
local __f = redis.call('GET', KEYS[1])
if __f and tonumber(__f) > tonumber(ARGV[1]) then return -2 end
-- round key registry (same hash tag as the fence key): lets purgeRoundData delete without scanning the keyspace
local __reg = string.gsub(KEYS[1], ':fence$', ':keys')
-- KEYS[2]=stream; ARGV[2]=group. Only BUSYGROUP (group already exists) is ignored; any other error (OOM, ...) is raised.
local r = redis.pcall('XGROUP', 'CREATE', KEYS[2], ARGV[2], '0', 'MKSTREAM')
if type(r) == 'table' and r.err and not string.find(r.err, 'BUSYGROUP', 1, true) then
  return redis.error_reply(r.err)
end
redis.call('SADD', __reg, KEYS[2])
return 1
