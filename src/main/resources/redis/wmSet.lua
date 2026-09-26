-- KEYS[1] watermarks
-- ARGV[1] partition  ARGV[2] generation (consumer group generation id)  ARGV[3] eventTimeMillis
-- Rejects a lower generation than stored. The same generation keeps max(stored, new). A higher
-- generation overwrites. updatedAt is Redis TIME. Returns 1 if stored, 0 if rejected.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local gen = tonumber(ARGV[2])
local et = tonumber(ARGV[3])
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur then
  local g, e = string.match(cur, '^(-?%d+)|(-?%d+)|')
  g = tonumber(g)
  e = tonumber(e)
  if gen < g then return 0 end
  if gen == g and e > et then et = e end
end
redis.call('HSET', KEYS[1], ARGV[1], string.format('%.0f|%.0f|%.0f', gen, et, now))
return 1
