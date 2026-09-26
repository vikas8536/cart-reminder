-- KEYS[1] timers:{s}   KEYS[2] timerdata:{s}
-- ARGV[1] cartId  ARGV[2] version (non-negative decimal)
-- Removes the timer only if its stored version is <= ARGV[2]. Returns 1 or 0.
local function cmpver(a, b)
  if #a ~= #b then return (#a < #b) and -1 or 1 end
  if a == b then return 0 end
  return (a < b) and -1 or 1
end

local cur = redis.call('HGET', KEYS[2], ARGV[1])
if not cur then return 0 end
local v = string.match(cur, '^[^|]*|(%d+)|')
if cmpver(v, ARGV[2]) > 0 then return 0 end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
