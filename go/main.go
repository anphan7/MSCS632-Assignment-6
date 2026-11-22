// Package main implements a concurrent data processing system.
// A producer fills a shared TaskQueue (a channel), worker goroutines pull
// tasks from it, process them, and save results to a shared ResultStore
// (an in-memory slice plus an output file).
package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Task is a unit of work. It is passed by value through the channel, so
// each worker gets its own copy and no locking is needed on it.
type Task struct {
	ID      int
	Payload string
}

// Result is the outcome of one processed task, including which worker handled it.
type Result struct {
	TaskID int
	Worker string
	Output string
}

// ErrQueueClosed is returned by GetTask when the queue is empty and closed (no work left).
var ErrQueueClosed = errors.New("queue is empty and closed")

// TaskQueue is the shared queue. A Go channel is already safe for concurrent
// use, so it needs no mutex: each task is received by exactly one goroutine.
type TaskQueue struct {
	tasks chan Task
}

// NewTaskQueue creates a buffered channel so the producer can add tasks without blocking.
func NewTaskQueue(size int) *TaskQueue {
	return &TaskQueue{tasks: make(chan Task, size)}
}

// AddTask puts a task on the queue.
func (q *TaskQueue) AddTask(t Task) {
	q.tasks <- t
}

// GetTask blocks until a task is available. When the channel is closed and
// drained, the receive returns ok == false and we report ErrQueueClosed.
func (q *TaskQueue) GetTask() (Task, error) {
	t, ok := <-q.tasks
	if !ok {
		return Task{}, ErrQueueClosed
	}
	return t, nil
}

// Close signals that no more tasks will be added; waiting workers are released.
func (q *TaskQueue) Close() {
	close(q.tasks)
}

// ResultStore is the shared output resource. The slice and the file writer
// are not safe for concurrent use, so a mutex guards both.
type ResultStore struct {
	mu      sync.Mutex
	results []Result
	writer  *bufio.Writer
}

// Save appends a result to the slice and writes it to the file.
func (s *ResultStore) Save(r Result) error {
	s.mu.Lock()
	// defer guarantees the mutex is released on every return path.
	defer s.mu.Unlock()
	s.results = append(s.results, r)
	_, err := fmt.Fprintf(s.writer, "task=%d worker=%s result=%s\n", r.TaskID, r.Worker, r.Output)
	// The write error (if any) is returned to the caller instead of being ignored.
	return err
}

// Count returns how many results were saved.
func (s *ResultStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.results)
}

// process simulates computational work on one task.
func process(t Task) (string, error) {
	// Sleep 100-300 ms (varies by task id) to represent processing time.
	time.Sleep(time.Duration(100+(t.ID*37)%200) * time.Millisecond)
	// Empty payloads are invalid input; return an error instead of a result.
	if strings.TrimSpace(t.Payload) == "" {
		return "", fmt.Errorf("task %d has an empty payload", t.ID)
	}
	// The "work": upper-case the text and count its characters and vowels.
	upper := strings.ToUpper(t.Payload)
	vowels := 0
	for _, c := range upper {
		if strings.ContainsRune("AEIOU", c) {
			vowels++
		}
	}
	return fmt.Sprintf("%s (chars=%d, vowels=%d)", upper, len(upper), vowels), nil
}

// worker runs in its own goroutine and keeps taking tasks until the queue is closed.
func worker(name string, queue *TaskQueue, store *ResultStore, failures *int, failMu *sync.Mutex, wg *sync.WaitGroup) {
	// Tell the WaitGroup this worker is finished, no matter how the function exits.
	defer wg.Done()
	log.Printf("[INFO   ] %s started", name)
	done := 0
	// Deferred so the completion message is always logged when the worker exits.
	defer func() { log.Printf("[INFO   ] %s completed (%d tasks)", name, done) }()

	for {
		// Step 1: get the next task; ErrQueueClosed means all work is done.
		task, err := queue.GetTask()
		if errors.Is(err, ErrQueueClosed) {
			return
		}
		// Step 2: process the task, then save the result only if processing succeeded.
		output, err := process(task)
		if err == nil {
			err = store.Save(Result{task.ID, name, output})
		}
		// Step 3: on any error, log and count it, then continue with the next task.
		if err != nil {
			failMu.Lock()
			*failures++
			failMu.Unlock()
			log.Printf("[ERROR  ] %s error: %v", name, err)
			continue
		}
		done++
		log.Printf("[INFO   ] %s processed task %d", name, task.ID)
	}
}

// run sets up the queue, workers, and output file, and returns any fatal error.
func run(workerCount int, outPath string) error {
	file, err := os.Create(outPath)
	if err != nil {
		// %w wraps the original error so the caller can still inspect the cause.
		return fmt.Errorf("cannot open output file: %w", err)
	}
	// Close the file when run returns, on success or on error.
	defer file.Close()

	store := &ResultStore{writer: bufio.NewWriter(file)}
	// Sample data; the two empty strings deliberately trigger the error-handling path.
	payloads := []string{"alpha", "bravo", "charlie", "delta", "", "echo", "foxtrot",
		"golf", "hotel", "india", "", "juliet"}

	queue := NewTaskQueue(len(payloads))
	// WaitGroup tracks running workers so main can wait for all of them to finish.
	var wg sync.WaitGroup
	// failMu protects the shared failure counter.
	var failMu sync.Mutex
	failures := 0

	// Start one goroutine per worker.
	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go worker(fmt.Sprintf("worker-%d", i), queue, store, &failures, &failMu, &wg)
	}
	// Producer: add every task, then close the queue so workers know when to stop.
	for i, p := range payloads {
		queue.AddTask(Task{i + 1, p})
	}
	queue.Close()
	// Block until every worker has called wg.Done().
	wg.Wait()

	// Push buffered results to disk and report a failure if the flush does not succeed.
	if err := store.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush output file: %w", err)
	}
	log.Printf("[INFO   ] done: %d tasks, %d succeeded, %d failed, results in %s",
		len(payloads), store.Count(), failures, outPath)
	return nil
}

func main() {
	// Log format: time with microseconds, matching the Java output.
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// Optional command-line argument: number of worker goroutines (default 4).
	workerCount := 4
	if len(os.Args) > 1 {
		n, err := strconv.Atoi(os.Args[1])
		if err != nil || n < 1 {
			log.Fatalf("[ERROR  ] invalid worker count %q", os.Args[1])
		}
		workerCount = n
	}
	// Errors from run are logged and the program exits with a non-zero status.
	if err := run(workerCount, "results_go.txt"); err != nil {
		log.Fatalf("[ERROR  ] %v", err)
	}
}
