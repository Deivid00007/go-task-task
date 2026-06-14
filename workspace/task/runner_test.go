package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunner_Run(t *testing.T) {
	runner := NewRunner()

	t1 := NewTask("1", func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	t2 := NewTask("2", func(ctx context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return errors.New("failed")
	})

	runner.AddTask(t1)
	runner.AddTask(t2)

	runner.Run(context.Background())

	if t1.State != StateCompleted {
		t.Errorf("expected t1 to be Completed, got %s", t1.State)
	}
	if t2.State != StateFailed {
		t.Errorf("expected t2 to be Failed, got %s", t2.State)
	}
}

func TestRunner_Run_Concurrent(t *testing.T) {
	runner := NewRunner()
	numTasks := 100

	var tasks []*Task
	for i := 0; i < numTasks; i++ {
		task := NewTask(fmt.Sprintf("%d", i), func(ctx context.Context) error {
			time.Sleep(5 * time.Millisecond)
			return nil
		})
		runner.AddTask(task)
		tasks = append(tasks, task)
	}

	stopChan := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				for _, task := range tasks {
					_ = task.GetState()
					_, _ = task.GetMetadata("key")
				}
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	runner.Run(context.Background())
	close(stopChan)
	wg.Wait()

	for _, task := range tasks {
		if state := task.GetState(); state != StateCompleted {
			t.Errorf("expected task %s to be Completed, got %s", task.ID, state)
		}
	}
}

func TestRunner_Run_Cancel(t *testing.T) {
	runner := NewRunner()
	ctx, cancel := context.WithCancel(context.Background())

	t1 := NewTask("1", func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	runner.AddTask(t1)

	cancel()
	runner.Run(ctx)

	if state := t1.GetState(); state != StateFailed {
		t.Errorf("expected t1 to be Failed due to cancellation, got %s", state)
	}
	if err := t1.GetErr(); err != context.Canceled {
		t.Errorf("expected t1 error to be context.Canceled, got %v", err)
	}
}

func TestRunner_Run_DoesNotStartRunningTaskTwice(t *testing.T) {
	runner := NewRunner()

	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})

	task := NewTask("1", func(ctx context.Context) error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return nil
	})
	runner.AddTask(task)

	firstDone := make(chan struct{})
	secondDone := make(chan struct{})

	go func() {
		runner.Run(context.Background())
		close(firstDone)
	}()

	<-started

	go func() {
		runner.Run(context.Background())
		close(secondDone)
	}()

	select {
	case <-secondDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("second Run should skip the already-running task")
	}

	close(release)

	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first Run did not finish")
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected task action to run once, got %d", got)
	}

	if state := task.GetState(); state != StateCompleted {
		t.Fatalf("expected task to complete, got %s", state)
	}
}
