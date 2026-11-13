-- KEYS[1]=split index set, KEYS[2]=split queue; ARGV[1]=split id, ARGV[2]=split json
if redis.call('SADD', KEYS[1], ARGV[1]) == 1 then
  redis.call('XADD', KEYS[2], '*', 's', ARGV[2])
  return 1
end
return 0
