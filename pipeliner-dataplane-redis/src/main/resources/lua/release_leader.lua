-- KEYS[1]=leader key; ARGV[1]=pod id; deletes only if ours
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
