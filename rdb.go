package tq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// defaultQueueName is the name of the queue this broker serves.
const defaultQueueName = "default"

// Job is a live task: the runtime instance created when a task is enqueued.
// A worker receives a Job and reads the task definition (Type, Payload) plus
// its runtime identity (ID).
type Job struct {
	JobBody

	status string
	qname  string
}

// JobBody is the serialized form of a Job's exported fields, stored as the
// "body" field of the job hash.
//
// A worker unmarshals it back into a Job after dequeue.
type JobBody struct {
	ID      uuid.UUID `json:"id"`
	Type    string    `json:"type"`
	Payload []byte    `json:"payload"`
	Opts    options   `json:"opts"`
}

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
	keyStatusFailed     = "tq:{%s}:job:failed"    // qname      ZSet, score = failedAt
	keyStatusScheduled  = "tq:{%s}:job:scheduled"  // qname      ZSet, score = scheduledAt
	keyStatusRetry      = "tq:{%s}:job:retry"      // qname      ZSet, score = nextRetryAt
)

func jobKey(j *Job) string             { return fmt.Sprintf(keyJob, j.qname, j.ID) }
func pendingKey(qname string) string   { return fmt.Sprintf(keyStatusPending, qname) }
func runningKey(qname string) string   { return fmt.Sprintf(keyStatusRunning, qname) }
func completedKey(qname string) string { return fmt.Sprintf(keyStatusCompleted, qname) }
func failedKey(qname string) string    { return fmt.Sprintf(keyStatusFailed, qname) }
func scheduledKey(qname string) string { return fmt.Sprintf(keyStatusScheduled, qname) }
func retryKey(qname string) string     { return fmt.Sprintf(keyStatusRetry, qname) }

const statsTTL = 90 * 24 * time.Hour // 90 days

// leaseDuration is the default lease duration of a running job; once it
// expires, the job can be recovered and re-scheduled by a recovery process.
const leaseDuration = 30 * time.Second // 30s

// RDB wraps a redis client and implements the task persistence layer.
// Embedding the interface promotes all redis methods onto b.
type RDB struct {
	client redis.UniversalClient
}

func NewRDB(client redis.UniversalClient) *RDB {
	return &RDB{client: client}
}

// enqueue adds the given task to the pending list of the queue.
func (r *RDB) enqueue(ctx context.Context, qname string, t *Task) error {
	script := redis.NewScript(`
local job_key = KEYS[1]
local pending_queue = KEYS[2]

local job_id          = ARGV[1]
local now             = ARGV[2]
local job_body        = ARGV[3]
local field_body      = ARGV[4]
local field_status    = ARGV[5]
local pending_status  = ARGV[6]
local field_pending   = ARGV[7]

if redis.call("EXISTS", job_key) == 1 then
	return 0
end
redis.call("HSET", job_key,
	   field_body, job_body,
           field_status, pending_status,
           field_pending, now)
redis.call("LPUSH", pending_queue, job_id)
return 1
`)
	jobID := uuid.New()
	var o options
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

	keys := []string{
		fmt.Sprintf(keyJob, qname, jobID),
		pendingKey(qname),
	}
	argv := []interface{}{
		jobID.String(),
		time.Now().Unix(),
		string(body),
		fieldBody,
		fieldStatus,
		StatusPending.String(),
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
		time.Now().Add(leaseDuration).Unix(),
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

// decodeJob translates from hash field in redis (tq:{<qname>}:job:<id>) to Job
// struct.
func decodeJob(qname string, fields map[string]string) (*Job, error) {
	body, ok := fields[fieldBody]
	if !ok {
		return nil, fmt.Errorf("decodeJob: missing field %q", fieldBody)
	}
	var jb JobBody
	if err := json.Unmarshal([]byte(body), &jb); err != nil {
		return nil, err
	}
	return &Job{
		JobBody: jb,
		status:  fields[fieldStatus],
		qname:   qname,
	}, nil
}
