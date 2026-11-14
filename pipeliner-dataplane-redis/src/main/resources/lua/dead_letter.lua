-- KEYS[1]=input stream, KEYS[2]=dlq, KEYS[3]=journal
-- Does nothing (returns 0) when the entry is no longer pending (already committed or dead-lettered) or is pending for another consumer.
-- ARGV[1]=group, ARGV[2]=entry id, ARGV[3]=error, ARGV[4]=event json, ARGV[5]=dead-lettering consumer ('' = no ownership check),
-- ARGV[6..]=original field name/value pairs
if ARGV[5] ~= '' then
  local p = redis.pcall('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
  if type(p) ~= 'table' or p.err or #p == 0 or p[1][2] ~= ARGV[5] then return 0 end
end
if redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]) == 0 then return 0 end
redis.call('XDEL', KEYS[1], ARGV[2])
local args = {'XADD', KEYS[2], '*', 'src', KEYS[1]}
for i = 6, #ARGV do args[#args + 1] = ARGV[i] end
args[#args + 1] = 'err'
args[#args + 1] = ARGV[3]
redis.call(unpack(args))
local __reg = string.gsub(KEYS[3], ':journal$', ':keys')
redis.call('SADD', __reg, KEYS[2])
if ARGV[4] ~= '' then
  redis.call('XADD', KEYS[3], '*', 'e', ARGV[4])
  redis.call('SADD', __reg, KEYS[3])
end
return 1
