package task

import (
	"context"
	"sync"
	"time"
)

type State string

const (
	StatePending   State = "Pending"
	StateRunning   State = "Running"
	StateCompleted State = "Completed"
	StateFailed    State = "Failed"
)

type Task struct {
	mu         sync.RWMutex
	ID         string
	State      State
	Action     func(ctx context.Context) error
	Err        error
	Metadata   map[string]interface{}
	History    []State
	StartedAt  time.Time
	FinishedAt time.Time
}

func NewTask(id string, action func(ctx context.Context) error) *Task {
	return &Task{
		ID:       id,
		State:    StatePending,
		Action:   action,
		Metadata: make(map[string]interface{}),
		History:  []State{StatePending},
	}
}

func (t *Task) GetState() State {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.State
}

func (t *Task) SetState(state State) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.State = state
	t.History = append(t.History, state)
}

func (t *Task) GetErr() error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Err
}

func (t *Task) SetErr(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Err = err
}

func (t *Task) GetMetadata(key string) (interface{}, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	val, ok := t.Metadata[key]
	return val, ok
}

func (t *Task) SetMetadata(key string, val interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Metadata[key] = val
}

func (t *Task) start(ctx context.Context, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.State == StateRunning {
		return false
	}

	if err := ctx.Err(); err != nil {
		t.State = StateFailed
		t.Err = err
		t.FinishedAt = now
		t.History = append(t.History, StateFailed)
		return false
	}

	t.State = StateRunning
	t.StartedAt = now
	t.History = append(t.History, StateRunning)
	return true
}

func (t *Task) finish(err error, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.FinishedAt = now
	if err != nil {
		t.State = StateFailed
		t.Err = err
		t.History = append(t.History, StateFailed)
		return
	}

	t.State = StateCompleted
	t.Err = nil
	t.History = append(t.History, StateCompleted)
}

func (t *Task) GetHistory() []State {
	t.mu.RLock()
	defer t.mu.RUnlock()
	h := make([]State, len(t.History))
	copy(h, t.History)
	return h
}

func (t *Task) GetStartedAt() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.StartedAt
}

func (t *Task) GetFinishedAt() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.FinishedAt
}
