package tq

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/google/uuid"
)

// Job is a live task: the runtime instance created when a task is enqueued.
// A worker receives a Job and reads the task definition (Type, Payload) plus
// its runtime identity (ID).
type Job struct {
	JobBody

	status  string
	qname   string
	retried int // number of retries so far; persisted in the job hash
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

// decodeJob translates from hash field in redis (tq:{<qname>}:job:<id>) to Job struct.
func decodeJob(qname string, fields map[string]string) (*Job, error) {
	body, ok := fields[fieldBody]
	if !ok {
		return nil, fmt.Errorf("decodeJob: missing field %q", fieldBody)
	}
	var jb JobBody
	if err := json.Unmarshal([]byte(body), &jb); err != nil {
		return nil, err
	}
	retried, _ := strconv.Atoi(fields[fieldRetried])
	return &Job{
		JobBody: jb,
		status:  fields[fieldStatus],
		qname:   qname,
		retried: retried,
	}, nil
}
