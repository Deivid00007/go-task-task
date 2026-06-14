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

			if !task.start(ctx, time.Now()) {
				return
			}

			err := task.Action(ctx)
			task.finish(err, time.Now())
		}(t)
	}
	wg.Wait()
}
