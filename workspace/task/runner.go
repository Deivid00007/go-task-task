package task

import (
	"context"
	"sync"
	"time"
)

type Runner struct {
	mu    sync.RWMutex
	tasks []*Task
}

func NewRunner() *Runner {
	return &Runner{
		tasks: make([]*Task, 0),
	}
}

func (r *Runner) AddTask(t *Task) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tasks = append(r.tasks, t)
}

func (r *Runner) Run(ctx context.Context) {
	r.mu.RLock()
	tasks := make([]*Task, len(r.tasks))
	copy(tasks, r.tasks)
	r.mu.RUnlock()

	var wg sync.WaitGroup
	for _, t := range tasks {
		wg.Add(1)
		go func(task *Task) {
			defer wg.Done()

			if ctx.Err() != nil {
				task.mu.Lock()
				task.State = StateFailed
				task.Err = ctx.Err()
				task.History = append(task.History, StateFailed)
				task.mu.Unlock()
				return
			}

			task.mu.Lock()
			task.State = StateRunning
			task.StartedAt = time.Now()
			task.History = append(task.History, StateRunning)
			task.mu.Unlock()

			err := task.Action(ctx)

			task.mu.Lock()
			task.FinishedAt = time.Now()
			if err != nil {
				task.State = StateFailed
				task.Err = err
				task.History = append(task.History, StateFailed)
			} else {
				task.State = StateCompleted
				task.History = append(task.History, StateCompleted)
			}
			task.mu.Unlock()
		}(t)
	}
	wg.Wait()
}