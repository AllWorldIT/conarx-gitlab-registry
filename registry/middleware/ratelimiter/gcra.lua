local base_key = KEYS[1]
local capacity = tonumber(ARGV[1]) -- max tokens (burst)
local refill_rate = tonumber(ARGV[2]) -- tokens per second
local current_time = tonumber(ARGV[3])
local tokens_requested = tonumber(ARGV[4]) or 1

-- CRITICAL: Use the SAME key for storage, don't append anything
-- In Redis Cluster, base_key and base_key:state might go to different slots!
local bucket_key = base_key

-- Get current bucket state using the same key
local bucket_data = redis.call("HMGET", bucket_key, "tokens", "last_refill")
local stored_tokens = bucket_data[1]
local stored_last_refill = bucket_data[2]

local tokens = tonumber(stored_tokens) or capacity

-- Only do refill calculation if we have valid data
local last_refill = tonumber(stored_last_refill)
if last_refill and current_time > last_refill then
	local time_elapsed = current_time - last_refill
	local tokens_to_add = time_elapsed * refill_rate
	tokens = math.min(capacity, tokens + tokens_to_add)
end

-- Check if we can satisfy the request BEFORE consuming
local allowed = 0
if tokens >= tokens_requested then
	allowed = tokens_requested
	tokens = tokens - tokens_requested
else
	allowed = tokens
	tokens = 0
end

-- Calculate remaining AFTER consumption
local remaining = math.max(0, math.floor(tokens))

-- Calculate retry after (for 429 responses)
local retry_after = 0
if allowed == 0 then
	local tokens_needed = tokens_requested - tokens
	retry_after = math.ceil(tokens_needed / refill_rate)
end

-- Calculate reset time (seconds until bucket is full again)
local reset = 0
if tokens < capacity then
	local tokens_to_full = capacity - tokens
	reset = math.ceil(tokens_to_full / refill_rate)
end

-- Store state in the SAME key that was passed in (guaranteed same slot)
redis.call("HMSET", bucket_key, "tokens", tostring(tokens), "last_refill", tostring(current_time))
-- `reset` is the maximum time after which the bucket would have
-- filled including replenishing the Burst limit. After that there is no point in
-- keeping the key in Redis
redis.call("EXPIRE", bucket_key, reset)

-- Return: allowed, remaining, retry_after, reset
return { allowed, remaining, retry_after, reset }
