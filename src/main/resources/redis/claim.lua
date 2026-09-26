-- KEYS[1] timers:{s}   KEYS[2] timerdata:{s}
-- ARGV[1] max members  ARGV[2] leaseMs
-- Takes up to ARGV[1] members with score <= Redis TIME (lowest first), re-scores each to TIME + lease,
-- and returns a flat list {cartId1, packed1, cartId2, packed2, ...}. Index entries without data are dropped.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local leased = string.format('%.0f', now + tonumber(ARGV[2]))
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', string.format('%.0f', now), 'LIMIT', 0, tonumber(ARGV[1]))
local out = {}
for _, id in ipairs(ids) do
  local data = redis.call('HGET', KEYS[2], id)
  if data then
    redis.call('ZADD', KEYS[1], leased, id)
    out[#out + 1] = id
    out[#out + 1] = data
  else
    redis.call('ZREM', KEYS[1], id)
  end
end
return out
