-- KEYS[1]=input stream, KEYS[2]=dlq, KEYS[3]=journal
-- Does nothing (returns 0) when the entry is no longer pending (already committed or dead-lettered).
-- ARGV[1]=group, ARGV[2]=entry id, ARGV[3]=error, ARGV[4]=event json, ARGV[5..]=original field name/value pairs
if redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]) == 0 then return 0 end
redis.call('XDEL', KEYS[1], ARGV[2])
local args = {'XADD', KEYS[2], '*', 'src', KEYS[1]}
for i = 5, #ARGV do args[#args + 1] = ARGV[i] end
args[#args + 1] = 'err'
args[#args + 1] = ARGV[3]
redis.call(unpack(args))
if ARGV[4] ~= '' then redis.call('XADD', KEYS[3], '*', 'e', ARGV[4]) end
return 1
