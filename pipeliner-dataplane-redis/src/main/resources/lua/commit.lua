-- KEYS[1]=input stream, KEYS[2]=journal, KEYS[3..]=output streams
-- ARGV[1]=group, ARGV[2]=input entry id, ARGV[3]=high watermark, ARGV[4]=event json, ARGV[5..]=(output KEYS index, payload) pairs
local hw = tonumber(ARGV[3])
if hw > 0 then
  for i = 3, #KEYS do
    if redis.call('XLEN', KEYS[i]) >= hw then return -1 end
  end
end
if redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]) == 0 then return 0 end
redis.call('XDEL', KEYS[1], ARGV[2])
local i = 5
while i <= #ARGV do
  redis.call('XADD', KEYS[tonumber(ARGV[i])], '*', 'c', ARGV[i + 1])
  i = i + 2
end
if ARGV[4] ~= '' then redis.call('XADD', KEYS[2], '*', 'e', ARGV[4]) end
return 1
