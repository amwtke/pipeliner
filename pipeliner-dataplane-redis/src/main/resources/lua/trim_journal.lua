-- fence: KEYS[1]=pg:{r}:fence, ARGV[1]=master epoch; a higher stored epoch means this caller is a deposed master
local __f = redis.call('GET', KEYS[1])
if __f and tonumber(__f) > tonumber(ARGV[1]) then return -2 end
-- KEYS[2]=journal; ARGV[2]=min id, ARGV[3]=batch size.
-- XTRIM MINID (Redis 6.2) built from XRANGE + XDEL, which Redis 6.0 has: removes up to batch entries whose id is below min id
-- (the entry at min id stays) and returns how many it removed.
local min = ARGV[2]
if not string.find(min, '-', 1, true) then min = min .. '-0' end
local ids = {}
for _, e in ipairs(redis.call('XRANGE', KEYS[2], '-', min, 'COUNT', ARGV[3])) do
  if e[1] ~= min then ids[#ids + 1] = e[1] end
end
if #ids > 0 then redis.call('XDEL', KEYS[2], unpack(ids)) end
return #ids
