-- KEYS[2]=dlq; ARGV[2]=batch size; moves up to batch entries back to their src stream, returns count
-- fence: KEYS[1]=pg:{r}:fence, ARGV[1]=master epoch; a higher stored epoch means this caller is a deposed master
local __f = redis.call('GET', KEYS[1])
if __f and tonumber(__f) > tonumber(ARGV[1]) then return -2 end
local entries = redis.call('XRANGE', KEYS[2], '-', '+', 'COUNT', ARGV[2])
local n = 0
for _, e in ipairs(entries) do
  local f = e[2]
  local src = nil
  local args = {'XADD', '', '*'}
  for i = 1, #f, 2 do
    if f[i] == 'src' then src = f[i + 1]
    elseif f[i] ~= 'err' then
      args[#args + 1] = f[i]
      args[#args + 1] = f[i + 1]
    end
  end
  if src then
    args[2] = src
    redis.call(unpack(args))
  end
  redis.call('XDEL', KEYS[2], e[1])
  n = n + 1
end
return n
