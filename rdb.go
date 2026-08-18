package tq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RDB wraps a redis client and implements the task persistence layer.
// Embedding the interface promotes all redis methods onto b.
type RDB struct {
	client redis.UniversalClient

	// leaseDuration is the running-job lease duration (F12). It is
	// configurable so tests can use a short lease instead of waiting 30s.
	leaseDuration time.Duration
}

func NewRDB(client redis.UniversalClient) *RDB {
	return &RDB{client: client, leaseDuration: defaultLeaseDuration}
}

// defaultQueueName is the name of the queue this broker serves.
const defaultQueueName = "default"

// tq:{%s}:job:%s hash type's fields
const (
	fieldBody         = "body"
	fieldStatus       = "status"
	fieldPendingSince = "pending_since"
	fieldStartedAt    = "started_at"
	fieldFinishedAt   = "finished_at"
	fieldError        = "error"
	fieldRetried      = "retried"
	fieldResult       = "result"
)

const (
	keyJob             = "tq:{%s}:job:%s"        // qname, id  Hash
	keyStatusPending   = "tq:{%s}:job:pending"   // qname      List
	keyStatusRunning   = "tq:{%s}:job:running"   // qname      ZSet, score = leaseTime
	keyStatusCompleted = "tq:{%s}:job:completed" // qname      ZSet, score = completedAt
	keyStatusFailed    = "tq:{%s}:job:failed"    // qname      ZSet, score = failedAt
	keyStatusScheduled = "tq:{%s}:job:scheduled" // qname      ZSet, score = scheduledAt
	keyStatusRetry     = "tq:{%s}:job:retry"     // qname      ZSet, score = nextRetryAt
)

func jobKey(j *Job) string             { return fmt.Sprintf(keyJob, j.qname, j.ID) }
func pendingKey(qname string) string   { return fmt.Sprintf(keyStatusPending, qname) }
func runningKey(qname string) string   { return fmt.Sprintf(keyStatusRunning, qname) }
func completedKey(qname string) string { return fmt.Sprintf(keyStatusCompleted, qname) }
func failedKey(qname string) string    { return fmt.Sprintf(keyStatusFailed, qname) }
func scheduledKey(qname string) string { return fmt.Sprintf(keyStatusScheduled, qname) }
func retryKey(qname string) string     { return fmt.Sprintf(keyStatusRetry, qname) }

const statsTTL = 90 * 24 * time.Hour // 90 days

// defaultLeaseDuration is the default lease duration of a running job; once
// it expires, the job can be recovered and re-scheduled by the recovery loop.
const defaultLeaseDuration = 30 * time.Second

// enqueue adds the given task to the pending list of the queue.
func (r *RDB) enqueue(ctx context.Context, qname string, t *Task) error {
	script := redis.NewScript(`
local job_key          = KEYS[1]
local pending_queue    = KEYS[2]
local scheduled_queue  = KEYS[3]

local job_id            = ARGV[1]
local now               = ARGV[2]
local process_at        = ARGV[3]
local job_body          = ARGV[4]
local field_body        = ARGV[5]
local field_status      = ARGV[6]
local pending_status    = ARGV[7]
local scheduled_status  = ARGV[8]
local field_pending     = ARGV[9]

if redis.call("EXISTS", job_key) == 1 then
  return 0
end

if tonumber(process_at) > tonumber(now) then
  -- set delay
  redis.call("HSET", job_key,
             field_body, job_body,
             field_status, scheduled_status)
  redis.call("ZADD", scheduled_queue, process_at, job_id)
elseif tonumber(process_at) == tonumber(now) then
  redis.call("HSET", job_key,
             field_body, job_body,
             field_status, pending_status,
             field_pending, now)
  redis.call("LPUSH", pending_queue, job_id)
else -- tonumber(process_at) < tonumber(now)
  return error_reply("nagetive delay")
end
return 1
`)
	jobID := uuid.New()
	o := defaultOptions()
	for _, opt := range t.opts {
		opt(&o)
	}
	body, err := json.Marshal(JobBody{
		ID:      jobID,
		Type:    t.typ,
		Payload: t.payload,
		Opts:    o,
	})
	if err != nil {
		return err
	}

	now := time.Now()
	processAt := now.Add(o.Delay)

	keys := []string{
		fmt.Sprintf(keyJob, qname, jobID),
		pendingKey(qname),
		scheduledKey(qname),
	}
	argv := []interface{}{
		jobID.String(),
		now.Unix(),
		processAt.Unix(),
		string(body),
		fieldBody,
		fieldStatus,
		StatusPending.String(),
		StatusScheduled.String(),
		fieldPendingSince,
	}
	cmd := script.Run(ctx, r.client, keys, argv...)
	if err := cmd.Err(); err != nil {
		return err
	}
	return nil
}

// dequeue atomically moves a job from the pending queue to the running queue
// and returns it. The whole move runs in a single Lua script (blocking
// commands are not allowed there), so no other worker can ever observe an
// intermediate state. It returns a "NOT FOUND" error when the queue is empty.
func (r *RDB) dequeue(ctx context.Context, qname string) (*Job, error) {
	script := redis.NewScript(`
local pending_queue = KEYS[1]
local running_queue = KEYS[2]
local job_key_prefix = ARGV[1]
local lease_time = ARGV[2] -- lease expiration time in unix time
local field_status  = ARGV[3]
local running_status = ARGV[4]
local field_pending = ARGV[5]

local job_id = redis.call("RPOP", pending_queue)
if not job_id then
	return redis.error_reply("NOT FOUND")
end

local job_key = job_key_prefix .. job_id

redis.call("ZADD", running_queue, lease_time, job_id)
redis.call("HSET", job_key, field_status, running_status)
redis.call("HDEL", job_key, field_pending)

return redis.call("HGETALL", job_key)
`)
	keys := []string{
		pendingKey(qname),
		runningKey(qname),
	}
	argv := []interface{}{
		fmt.Sprintf(keyJob, qname, ""), // job key prefix, id is appended by the script
		time.Now().Add(r.leaseDuration).Unix(),
		fieldStatus,
		StatusRunning.String(),
		fieldPendingSince,
	}
	pairs, err := script.Run(ctx, r.client, keys, argv...).StringSlice()
	if err != nil {
		return nil, err
	}
	fields := make(map[string]string, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		fields[pairs[i]] = pairs[i+1]
	}
	return decodeJob(qname, fields)
}

func (r *RDB) markAsCompleted(ctx context.Context, job *Job) error {
	script := redis.NewScript(`
local running_queue    = KEYS[1]
local completed_queue  = KEYS[2]
local job_key          = KEYS[3]

local job_id            = ARGV[1]
local completed_at      = ARGV[2] -- score = completed time for janitor cleanup
local field_status      = ARGV[3]
local completed_status  = ARGV[4]

if redis.call("ZREM", running_queue, job_id) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", completed_queue, completed_at, job_id)
redis.call("HSET", job_key, field_status, completed_status)
return "OK"
`)
	keys := []string{
		runningKey(job.qname),
		completedKey(job.qname),
		jobKey(job),
	}
	args := []interface{}{
		job.ID.String(),
		time.Now().Unix(),
		fieldStatus,
		StatusCompleted.String(),
	}
	cmd := script.Run(ctx, r.client, keys, args...)
	if err := cmd.Err(); err != nil {
		return err
	}
	return nil
}

func (r *RDB) markAsFailed(ctx context.Context, job *Job, reason string) error {
	script := redis.NewScript(`
local running_queue = KEYS[1]
local failed_queue  = KEYS[2]
local job_key       = KEYS[3]

local job_id          = ARGV[1]
local failed_at       = ARGV[2] -- score = failed time for janitor cleanup
local reason          = ARGV[3]
local field_status    = ARGV[4]
local failed_status   = ARGV[5]
local field_error     = ARGV[6]

if redis.call("ZREM", running_queue, job_id) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", failed_queue, failed_at, job_id)
redis.call("HSET", job_key, field_status, failed_status, field_error, reason)
return "OK"
`)
	keys := []string{
		runningKey(job.qname),
		failedKey(job.qname),
		jobKey(job),
	}
	args := []interface{}{
		job.ID.String(),
		time.Now().Unix(),
		reason,
		fieldStatus,
		StatusFailed.String(),
		fieldError,
	}
	cmd := script.Run(ctx, r.client, keys, args...)
	if err := cmd.Err(); err != nil {
		return err
	}
	return nil
}

// markAsRetry moves a job from running to the retry zset, scheduled for
// nextRetryAt, and increments its retried counter. Atomic in one script.
func (r *RDB) markAsRetry(ctx context.Context, job *Job, nextRetryAt time.Time) error {
	script := redis.NewScript(`
local running_queue = KEYS[1]
local retry_queue   = KEYS[2]
local job_key       = KEYS[3]

local job_id          = ARGV[1]
local next_retry_at   = ARGV[2] -- score = time of the next retry
local field_status    = ARGV[3]
local retry_status    = ARGV[4]
local field_retried   = ARGV[5]

if redis.call("ZREM", running_queue, job_id) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", retry_queue, next_retry_at, job_id)
redis.call("HSET", job_key, field_status, retry_status)
redis.call("HINCRBY", job_key, field_retried, 1)
return "OK"
`)
	keys := []string{
		runningKey(job.qname),
		retryKey(job.qname),
		jobKey(job),
	}
	args := []interface{}{
		job.ID.String(),
		nextRetryAt.Unix(),
		fieldStatus,
		StatusRetry.String(),
		fieldRetried,
	}
	cmd := script.Run(ctx, r.client, keys, args...)
	if err := cmd.Err(); err != nil {
		return err
	}
	return nil
}

// forwardBatchSize caps how many delayed jobs a single forward call moves,
// so one Lua script can't monopolize Redis when a large backlog becomes due.
const forwardBatchSize = 100

// recoverBatchSize caps how many expired leases a single recover call moves,
// for the same reason: one Lua script must not monopolize Redis.
const recoverBatchSize = 100

// forward moves tasks whose scheduled/retry time (the zset score) has arrived
// from the delayed zsets into the pending list, where workers can dequeue them.
// It returns how many jobs were moved.
func (r *RDB) forward(ctx context.Context, qname string) (n int64, err error) {
	script := redis.NewScript(`
local scheduled_queue = KEYS[1]
local retry_queue     = KEYS[2]
local pending_queue   = KEYS[3]

local job_key_prefix  = ARGV[1]
local now             = ARGV[2]
local batch_size      = tonumber(ARGV[3])
local field_status    = ARGV[4]
local pending_status  = ARGV[5]
local field_pending   = ARGV[6]

local moved = 0
for _, delayed_queue in ipairs({scheduled_queue, retry_queue}) do
  local remaining = batch_size - moved
  if remaining <= 0 then break end

  local job_ids = redis.call("ZRANGEBYSCORE", delayed_queue, "-inf", now, "LIMIT", 0, remaining)
  for _, job_id in ipairs(job_ids) do
    -- ZREM returns 1 only to the caller that actually removes the member,
    -- so concurrent forwarders can never move the same job twice.
    if redis.call("ZREM", delayed_queue, job_id) == 1 then
      local job_key = job_key_prefix .. job_id
      redis.call("LPUSH", pending_queue, job_id)
      redis.call("HSET", job_key,
                  field_status, pending_status,
                  field_pending, now)
      moved = moved + 1
    end
  end
end

return moved
`)
	keys := []string{
		scheduledKey(qname),
		retryKey(qname),
		pendingKey(qname),
	}
	argv := []interface{}{
		fmt.Sprintf(keyJob, qname, ""), // job key prefix; id appended in-script
		time.Now().Unix(),
		forwardBatchSize,
		fieldStatus,
		StatusPending.String(),
		fieldPendingSince,
	}
	return script.Run(ctx, r.client, keys, argv...).Int64()
}

// tryMoveJobScript atomically moves one running job to a target queue if
// and only if its current running score still equals expectedScore. The score
// works like a version: if the job was already recovered, completed, or
// re-enqueued with a new lease, the score differs and the script does nothing.
// Redis executes the whole script atomically, so the check and the move cannot
// be interleaved by another client.
var tryMoveJobScript = redis.NewScript(`
-- KEYS[1]: running queue key
-- KEYS[2]: target queue key (retry or failed)
-- KEYS[3]: job hash key
-- ARGV[1]: job id
-- ARGV[2]: expected running score (the score the caller read earlier)
-- ARGV[3]: target score
-- ARGV[4]: new status
-- ARGV[5]: error message (empty string if none)
-- ARGV[6]: "1" to increment retried, "0" otherwise

local job_id         = ARGV[1]
local expected_score = tonumber(ARGV[2])

local current_score = redis.call("ZSCORE", KEYS[1], job_id)
if not current_score or tonumber(current_score) ~= expected_score then
  return 0
end

redis.call("ZREM", KEYS[1], job_id)
redis.call("ZADD", KEYS[2], tonumber(ARGV[3]), job_id)

if ARGV[6] == "1" then
  redis.call("HINCRBY", KEYS[3], "retried", 1)
end
redis.call("HSET", KEYS[3], "status", ARGV[4])
if ARGV[5] ~= "" then
  redis.call("HSET", KEYS[3], "error", ARGV[5])
end

return 1
`)

// recover reclaims running jobs whose lease has expired (running zset score <= now).
//
// It follows the same structure as asynq's recoverer, but with one stricter
// guard:
//  1. read expired job IDs and their current scores from the running zset;
//  2. for each job, load it and decide retry vs final failure in Go;
//  3. apply the decision with tryMoveJobScript, which verifies that the
//     running score is still exactly the score we read before moving it.
//
// The retry path increments the retried counter and applies the same backoff
// as settle. The failure path writes "lease expired": the worker is dead, so
// the recoverer writes its own reason rather than a handler error.
func (r *RDB) recover(ctx context.Context, qname string) (n int64, err error) {
	now := time.Now().Unix()
	zs, err := r.client.ZRangeByScoreWithScores(ctx, runningKey(qname), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(now, 10),
		Count: recoverBatchSize,
	}).Result()
	if err != nil {
		return 0, err
	}

	for _, z := range zs {
		jobID := z.Member.(string)
		expectedScore := int64(z.Score)

		fields, err := r.client.HGetAll(ctx, fmt.Sprintf(keyJob, qname, jobID)).Result()
		if err != nil {
			continue // single-key read error; next recovery pass will retry
		}
		job, err := decodeJob(qname, fields)
		if err != nil {
			continue // job hash is gone or malformed; nothing to recover
		}

		var targetQueueKey string
		var targetScore int64
		var newStatus string
		var errorMsg string
		incrRetried := false

		if job.Opts.MaxRetries > 0 && job.retried < job.Opts.MaxRetries {
			targetQueueKey = retryKey(qname)
			targetScore = time.Now().Add(backoff(job.retried)).Unix()
			newStatus = StatusRetry.String()
			incrRetried = true
		} else {
			targetQueueKey = failedKey(qname)
			targetScore = now
			newStatus = StatusFailed.String()
			errorMsg = "lease expired"
		}

		argv := []interface{}{jobID, expectedScore, targetScore, newStatus, errorMsg, "0"}
		if incrRetried {
			argv[5] = "1"
		}
		res, err := tryMoveJobScript.Run(ctx, r.client,
			[]string{runningKey(qname), targetQueueKey, fmt.Sprintf(keyJob, qname, jobID)},
			argv...).Int()
		if err != nil {
			return n, err
		}
		if res == 1 {
			n++
		}
	}
	return n, nil
}
