-- KEYS[1] watermarks
-- ARGV[1] staleAfterMs  ARGV[2..n] partitions
-- Returns {minEventTimeMillis, nowMillis} as strings. A partition that is missing, or whose
-- updatedAt is more than staleAfterMs before Redis TIME, counts as 0.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local stale = tonumber(ARGV[1])
local min = nil
for i = 2, #ARGV do
  local et = 0
  local cur = redis.call('HGET', KEYS[1], ARGV[i])
  if cur then
    local e, u = string.match(cur, '^-?%d+|(-?%d+)|(-?%d+)$')
    if now - tonumber(u) <= stale then et = tonumber(e) end
  end
  if min == nil or et < min then min = et end
end
if min == nil then min = 0 end
return {string.format('%.0f', min), string.format('%.0f', now)}
