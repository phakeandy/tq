package main

import (
	"context"
	"sync"
)

type Handler func(ctx context.Context, task *Task) error

var (
	handlers   = make(map[string]Handler)
	handlersMu sync.RWMutex
)

// RegisterHandler registers a handler for a given task type.
// Must be called before RunWorker.
func RegisterHandler(taskType string, h Handler) {
	handlersMu.Lock()
	defer handlersMu.Unlock()
	handlers[taskType] = h
}


