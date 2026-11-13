-- KEYS[1]=dlq; ARGV[1]=batch size; moves up to batch entries back to their src stream, returns count
local entries = redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', ARGV[1])
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
  redis.call('XDEL', KEYS[1], e[1])
  n = n + 1
end
return n
