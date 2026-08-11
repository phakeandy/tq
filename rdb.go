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
//
// It's export fileds are definited as job's body.
type Job struct {
	ID      uuid.UUID
	Type    string
	Payload []byte

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
}

const (
	keyJob             = "tq:{%s}:job:%s"        // qname, id  Hash
	keyStatusPending   = "tq:{%s}:job:pending"   // qname      List
	keyStatusRunning   = "tq:{%s}:job:running"   // qname      ZSet, score = leaseTime
	keyStatusCompleted = "tq:{%s}:job:completed" // qname      ZSet, score = completedAt
	keyStatusFailed    = "tq:{%s}:job:failed"    // qname      ZSet, score = failedAt
)

func jobKey(j *Job) string             { return fmt.Sprintf(keyJob, j.qname, j.ID) }
func pendingKey(qname string) string   { return fmt.Sprintf(keyStatusPending, qname) }
func runningKey(qname string) string   { return fmt.Sprintf(keyStatusRunning, qname) }
func completedKey(qname string) string { return fmt.Sprintf(keyStatusCompleted, qname) }
func failedKey(qname string) string    { return fmt.Sprintf(keyStatusFailed, qname) }

const statsTTL = 90 * 24 * time.Hour // 90 days

// leaseDuration is the default lease duration of a running job; once it
// expires, the job can be recovered and re-scheduled by a recovery process.
const leaseDuration = 30 * time.Second

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
local job_body = KEYS[2]
local pending_queue = KEYS[3]

local job_id  = ARGV[1]
local now     = ARGV[2]

if redis.call("EXISTS", job_key) == 1 then
	return 0
end
redis.call("HSET", job_key,
	   "body", job_body,
           "status", "pending",
           "pending_since", now)
redis.call("LPUSH", pending_queue, job_id)
return 1
`)
	job := &Job{
		ID:      uuid.New(),
		Type:    t.typ,
		Payload: t.payload,
		qname:   qname,
	}
	body, err := json.Marshal(JobBody{
		ID:      job.ID,
		Type:    job.Type,
		Payload: job.Payload,
	})
	if err != nil {
		return err
	}

	keys := []string{
		jobKey(job),
		string(body),
		pendingKey(qname),
	}
	argv := []interface{}{
		job.ID.String(),
		time.Now().Unix(),
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

local job_id = redis.call("RPOP", pending_queue)
if not job_id then
	return redis.error_reply("NOT FOUND")
end

local job_key = job_key_prefix .. job_id

redis.call("ZADD", running_queue, lease_time, job_id)
redis.call("HSET", job_key, "status", "running")
redis.call("HDEL", job_key, "pending_since")

return redis.call("HGETALL", job_key)
`)
	keys := []string{
		pendingKey(qname),
		runningKey(qname),
	}
	argv := []interface{}{
		fmt.Sprintf(keyJob, qname, ""), // job key prefix, id is appended by the script
		time.Now().Add(leaseDuration).Unix(),
	}
	pairs, err := script.Run(ctx, r.client, keys, argv...).StringSlice()
	if err != nil {
		return nil, err
	}
	fields := make(map[string]string, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		fields[pairs[i]] = pairs[i+1]
	}
	body, ok := fields["body"]
	if !ok {
		return nil, fmt.Errorf("dequeue: job hash has no body field")
	}
	var jb JobBody
	if err := json.Unmarshal([]byte(body), &jb); err != nil {
		return nil, err
	}
	return &Job{
		ID:      jb.ID,
		Type:    jb.Type,
		Payload: jb.Payload,
		status:  StatusRunning.String(),
		qname:   qname,
	}, nil
}
func (r *RDB) markAsCompleted(ctx context.Context, job *Job) error {
	script := redis.NewScript(`
local running_queue    = KEYS[1]
local completed_queue  = KEYS[2]
local job_key          = KEYS[3]

local job_id        = ARGV[1]
local job_exp_time  = ARGV[2] -- job expiration time in unix time

if redis.call("ZREM", running_queue, job_id) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZADD", completed_queue, job_exp_time, job_id) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("HSET", job_key, "status", "completed")
return "OK"
`)
	keys := []string{
		runningKey(job.qname),
		completedKey(job.qname),
		jobKey(job),
	}
	args := []interface{}{
		job.ID.String(),
		time.Now().Add(statsTTL).Unix(),
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

local job_id        = ARGV[1]
local job_exp_time  = ARGV[2] -- job expiration time in unix time
local reason        = ARGV[3]

if redis.call("ZREM", running_queue, job_id) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZADD", failed_queue, job_exp_time, job_id) == 0 then
  return redis.error_reply("NOT FOUND")
end

redis.call("HSET", job_key, "status", "failed", "error", reason)
return "OK"
`)
	keys := []string{
		runningKey(job.qname),
		failedKey(job.qname),
		jobKey(job),
	}
	args := []interface{}{
		job.ID.String(),
		time.Now().Add(statsTTL).Unix(),
		reason,
	}
	cmd := script.Run(ctx, r.client, keys, args...)
	if err := cmd.Err(); err != nil {
		return err
	}
	return nil
}
