-- KEYS[1]=leader key; ARGV[1]=pod id, ARGV[2]=ttl ms; returns 1 if acquired or renewed
local cur = redis.call('GET', KEYS[1])
if not cur then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
elseif cur == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
