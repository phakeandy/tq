package taskqueue

import (
	"context"

	"github.com/google/uuid"
	"github.com/redis/go-redis"
)

// Storer ...
type Storer struct {
	rdb redis.UniversalClient
}

func (s Store) Enqueue(ctx context.Context, t *Task)                               {}
func (s Store) Dequeue(ctx context.Context, t *Task) (taskID uuid.UUID, err error) {}

