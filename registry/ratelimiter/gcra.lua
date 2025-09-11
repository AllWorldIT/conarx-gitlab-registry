local base_key = KEYS[1]
local capacity = tonumber(ARGV[1])     -- max tokens (burst)
local refill_rate = tonumber(ARGV[2])  -- tokens per second
local current_time = tonumber(ARGV[3])
local tokens_requested = tonumber(ARGV[4]) or 1

-- CRITICAL: Use the SAME key for storage, don't append anything
-- In Redis Cluster, base_key and base_key:state might go to different slots!
local bucket_key = base_key

-- Get current bucket state using the same key
local bucket_data = redis.call('HMGET', bucket_key, 'tokens', 'last_refill')
local stored_tokens = bucket_data[1]
local stored_last_refill = bucket_data[2]

local tokens

-- Simple initialization: avoid all time calculations on first request
if stored_tokens == false or stored_tokens == nil or stored_tokens == '' then
    -- First time - start with full capacity, no time calculations
    tokens = capacity
else
    -- Parse existing tokens
    tokens = tonumber(stored_tokens)
    local last_refill = tonumber(stored_last_refill)

    -- Only do refill calculation if we have valid data
    if tokens and last_refill and current_time > last_refill then
        local time_elapsed = current_time - last_refill
        local tokens_to_add = time_elapsed * refill_rate
        tokens = math.min(capacity, tokens + tokens_to_add)
    elseif not tokens then
        -- Fallback: reinitialize if tokens is invalid
        tokens = capacity
    end
end

-- Check if request can be satisfied
local allowed = 0
local remaining = math.max(0, math.floor(tokens))

if tokens >= tokens_requested then
    allowed = 1
    tokens = tokens - tokens_requested
    remaining = math.max(0, math.floor(tokens))
end

-- Store state in the SAME key that was passed in (guaranteed same slot)
redis.call('HMSET', bucket_key, 'tokens', tostring(tokens), 'last_refill', tostring(current_time))
redis.call('EXPIRE', bucket_key, 3600)

-- Calculate retry after (for 429 responses)
local retry_after = 0
if allowed == 0 and refill_rate > 0 then
    local tokens_needed = tokens_requested - tokens
    retry_after = math.ceil(tokens_needed / refill_rate)
end

-- Calculate reset time (seconds until bucket is full again)
local reset = 0
if refill_rate > 0 and tokens < capacity then
    local tokens_to_full = capacity - tokens
    reset = math.ceil(tokens_to_full / refill_rate)
end

-- Return: allowed, remaining, retry_after, reset
return {allowed, remaining, retry_after, reset}