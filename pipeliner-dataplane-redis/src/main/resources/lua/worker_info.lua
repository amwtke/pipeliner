-- KEYS[1]=pg:worker:<pod>; ARGV[1]=ttl ms, ARGV[2..]=field/value pairs.
-- Replaces the whole hash atomically so fields the pod no longer reports (e.g. an old graph.<name>) disappear.
redis.call('DEL', KEYS[1])
if #ARGV >= 3 then
  local args = {'HSET', KEYS[1]}
  for i = 2, #ARGV do args[#args + 1] = ARGV[i] end
  redis.call(unpack(args))
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return 1
