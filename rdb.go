package tq

import (
	"context"
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
// status and qname are broker-managed and not part of the public contract.
type Job struct {
	ID      uuid.UUID
	Type    string
	Payload []byte

	status string
	qname  string
}

// Key returns the redis hash key under which this job's data is stored.
func (j Job) Key() string { return fmt.Sprintf(keyJob, j.qname, j.ID) }

const (
	keyJob             = "tq:{%s}:job:%s"        // qname, id  Hash
	keyStatusPending   = "tq:{%s}:job:pending"   // qname      List
	keyStatusRunning   = "tq:{%s}:job:running"   // qname      ZSet, score = leaseTime
	keyStatusCompleted = "tq:{%s}:job:completed" // qname      ZSet, score = completedAt
	keyStatusFailed    = "tq:{%s}:job:failed"    // qname      ZSet, score = failedAt
)

func pendingKey(qname string) string   { return fmt.Sprintf(keyStatusPending, qname) }
func runningKey(qname string) string   { return fmt.Sprintf(keyStatusRunning, qname) }
func completedKey(qname string) string { return fmt.Sprintf(keyStatusCompleted, qname) }
func failedKey(qname string) string    { return fmt.Sprintf(keyStatusFailed, qname) }

const statsTTL = 90 * 24 * time.Hour // 90 days

// RDB wraps a redis client and implements the task persistence layer.
// Embedding the interface promotes all redis methods onto b.
type RDB struct {
	client redis.UniversalClient
}

func NewRDB(client redis.UniversalClient) *RDB{
	return &RDB{client: client}
}

// enqueue adds the given task to the pending list of the queue.
func (r *RDB) enqueue(ctx context.Context, qname string, t *Task) error {
	// id := uuid.New()
	panic("TODO")

	// // Store the task data and push its id onto the pending queue in one
	// // pipeline so a partially written task is never visible to dequeuers.
	// pipe := b.Pipeline()
	// pipe.HSet(
	// 	ctx, taskKey(qname, id),
	// 	"type", t.typ,
	// 	"payload", t.payload,
	// )
	// pipe.LPush(ctx, pendingKey(qname), id.String())
	// _, err := pipe.Exec(ctx)
	// return err
}

// dequeue blocks until a job is available, atomically moving it from the
// pending queue to the running queue, and returns it.
func (r *RDB) dequeue(ctx context.Context, qname string) (*Job, error) {
	// TODO: use lua script
	// idStr, err := b.BLMove(
	// 	ctx,
	// 	pendingKey(qname), // pop from the tail: FIFO order
	// 	runningKey(qname), // push to the head
	// 	"RIGHT", "LEFT",
	// 	0, // block until a job is available or ctx is cancelled
	// ).Result()
	// if err != nil {
	// 	return nil, err
	// }

	// id, err := uuid.Parse(idStr)
	// if err != nil {
	// 	return nil, err
	// }

	// payload, err := b.HGet(ctx, taskKey(qname, id), "payload").Bytes()
	// if err != nil {
	// 	return nil, err
	// }

	panic("TODO")
	// return &Job{ID: id, Payload: payload, status: StatusRunning.String()}, nil
}
func (r *RDB) markAsCompleted(ctx context.Context, job *Job) error {
	script := redis.NewScript(`
local running_queue    = KEYS[1]
local completed_queue  = KEYS[2]
local job_key          = KEYS[3]

local job_id        = ARGV[1]
local job_exp_time  = ARGV[2] -- job expiration time in unix time

if redis.call("LREM", running_queue, 0, job_id) == 0 then
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
		job.Key(),
	}
	args := []interface{}{
		job.ID.String(),
		time.Now().Add(statsTTL).Unix(),
	}
	cmd := script.Run(ctx, r.client, keys, args...)
	if cmd.Err() != nil {
		fmt.Println(cmd.Err())
	}
	return nil
}

func (r *RDB) markAsFailed(ctx context.Context, id uuid.UUID, reason string) error {
	panic("TODO")
}
