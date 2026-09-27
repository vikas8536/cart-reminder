-- KEYS[1] timers:{s}   KEYS[2] timerdata:{s}
-- ARGV[1] cartId  ARGV[2] packed value as claimed  ARGV[3] delayMs
-- If the stored value still equals the claimed one, makes it due at Redis TIME + delay. Returns 1 or 0.
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('ZADD', KEYS[1], string.format('%.0f', now + tonumber(ARGV[3])), ARGV[1])
return 1
