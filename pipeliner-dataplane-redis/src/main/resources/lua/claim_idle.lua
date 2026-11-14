-- KEYS[1]=stream; ARGV[1]=group, ARGV[2]=consumer, ARGV[3]=min idle millis, ARGV[4]=start id (inclusive), ARGV[5]=count.
-- XAUTOCLAIM (Redis 6.2) built from XPENDING + XCLAIM, which Redis 6.0 has: looks at up to count*10 pending entries from the
-- start id and claims up to count of those idle for at least min idle.
-- Returns {next start id ('0-0' = scanned to the end), {entry id, delivery count, {field, value, ...}}, ...}.
-- A missing stream/group returns the NOGROUP error unchanged.
local count = tonumber(ARGV[5])
local window = count * 10
local minIdle = tonumber(ARGV[3])
local p = redis.pcall('XPENDING', KEYS[1], ARGV[1], ARGV[4], '+', window + 1)
if p.err then return p end
local out = {'0-0'}
for i, e in ipairs(p) do
  if i > window or #out > count then
    out[1] = e[1]   -- the next call resumes at this entry
    break
  end
  if e[3] >= minIdle then
    local c = redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], ARGV[3], e[1])
    if #c > 0 and c[1] then
      out[#out + 1] = {e[1], e[4] + 1, c[1][2]}   -- XCLAIM has just counted this delivery
    elseif #c > 0 then
      -- the entry was deleted from the stream and only its pending record is left (Redis < 7 answers nil): drop the record
      redis.call('XACK', KEYS[1], ARGV[1], e[1])
    end
  end
end
return out
