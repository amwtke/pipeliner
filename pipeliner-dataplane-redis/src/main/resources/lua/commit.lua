-- KEYS[1]=input stream, KEYS[2]=journal, KEYS[3..]=output streams
-- ARGV[1]=group, ARGV[2]=input entry id, ARGV[3]=high watermark, ARGV[4]=event json, ARGV[5]=committing consumer ('' = no ownership check),
-- ARGV[6..]=(output KEYS index, payload) pairs
-- Returns 1 ok, 0 conflict (not pending, or pending for another consumer: it was autoclaimed), -1 busy.
if ARGV[5] ~= '' then
  local p = redis.pcall('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
  if type(p) ~= 'table' or p.err or #p == 0 or p[1][2] ~= ARGV[5] then return 0 end
end
local hw = tonumber(ARGV[3])
if hw > 0 then
  for i = 3, #KEYS do
    if redis.call('XLEN', KEYS[i]) >= hw then return -1 end
  end
end
if redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]) == 0 then return 0 end
redis.call('XDEL', KEYS[1], ARGV[2])
local __reg = string.gsub(KEYS[2], ':journal$', ':keys')
local i = 6
while i <= #ARGV do
  local k = KEYS[tonumber(ARGV[i])]
  redis.call('XADD', k, '*', 'c', ARGV[i + 1])
  redis.call('SADD', __reg, k)
  i = i + 2
end
if ARGV[4] ~= '' then
  redis.call('XADD', KEYS[2], '*', 'e', ARGV[4])
  redis.call('SADD', __reg, KEYS[2])
end
return 1
