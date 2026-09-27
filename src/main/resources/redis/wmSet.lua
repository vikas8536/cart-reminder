-- KEYS[1] watermarks
-- ARGV[1] partition  ARGV[2] generation (consumer group generation id)  ARGV[3] eventTimeMillis  ARGV[4] staleAfterMs
-- Rejects a lower generation than stored while the stored entry is fresh. Once it is stale (Redis TIME - updatedAt >
-- staleAfterMs, the reader's window) a lower generation is accepted, so a consumer-group reset (generations restart
-- low) cannot fence a partition forever; safe because a writer's T is always backed by its own commit.
-- The same generation keeps max(stored, new). A higher generation overwrites. updatedAt is Redis TIME.
-- Returns 1 if stored, 0 if rejected.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local gen = tonumber(ARGV[2])
local et = tonumber(ARGV[3])
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur then
  local g, e, u = string.match(cur, '^(-?%d+)|(-?%d+)|(-?%d+)$')
  g = tonumber(g)
  e = tonumber(e)
  if gen < g and now - tonumber(u) <= tonumber(ARGV[4]) then return 0 end
  if gen == g and e > et then et = e end
end
redis.call('HSET', KEYS[1], ARGV[1], string.format('%.0f|%.0f|%.0f', gen, et, now))
return 1
