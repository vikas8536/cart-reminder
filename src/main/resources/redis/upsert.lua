-- KEYS[1] timers:{s}   KEYS[2] timerdata:{s}
-- ARGV[1] cartId  ARGV[2] packed kind|version|offsetIndex|srcPartition|dueAtMillis
-- ARGV[3] version (non-negative decimal, no leading zeros)  ARGV[4] offsetIndex (-1 for CHECK_ABANDON)
-- ARGV[5] dueAtMillis
-- Writes only if (version, offsetIndex) is greater than the stored pair. Equal is a no-op that keeps
-- the score, so a claimed timer keeps its lease. Returns {1, previous packed value or ''} if written,
-- {0, ''} otherwise.
-- Versions compare as decimal strings: Lua numbers are doubles and lose precision above 2^53.
local function cmpver(a, b)
  if #a ~= #b then return (#a < #b) and -1 or 1 end
  if a == b then return 0 end
  return (a < b) and -1 or 1
end

local cur = redis.call('HGET', KEYS[2], ARGV[1])
if cur then
  local v, o = string.match(cur, '^[^|]*|(%d+)|(-?%d+)|')
  local c = cmpver(ARGV[3], v)
  if c < 0 or (c == 0 and tonumber(ARGV[4]) <= tonumber(o)) then return {0, ''} end
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[5], ARGV[1])
return {1, cur or ''}
