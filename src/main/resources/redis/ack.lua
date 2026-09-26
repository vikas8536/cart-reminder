-- KEYS[1] timers:{s}   KEYS[2] timerdata:{s}
-- ARGV[1] cartId  ARGV[2] packed value as claimed
-- Removes the timer only if the stored value still equals the claimed one. Returns 1 or 0.
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
