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

type Task struct {
	ID      int
	Payload string
}

type Result struct {
	TaskID int
	Worker string
	Output string
}

var ErrQueueClosed = errors.New("queue is empty and closed")

type TaskQueue struct {
	tasks chan Task
}

func NewTaskQueue(size int) *TaskQueue {
	return &TaskQueue{tasks: make(chan Task, size)}
}

func (q *TaskQueue) AddTask(t Task) {
	q.tasks <- t
}

func (q *TaskQueue) GetTask() (Task, error) {
	t, ok := <-q.tasks
	if !ok {
		return Task{}, ErrQueueClosed
	}
	return t, nil
}

func (q *TaskQueue) Close() {
	close(q.tasks)
}

type ResultStore struct {
	mu      sync.Mutex
	results []Result
	writer  *bufio.Writer
}

func (s *ResultStore) Save(r Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, r)
	_, err := fmt.Fprintf(s.writer, "task=%d worker=%s result=%s\n", r.TaskID, r.Worker, r.Output)
	return err
}

func (s *ResultStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.results)
}

func process(t Task) (string, error) {
	time.Sleep(time.Duration(100+(t.ID*37)%200) * time.Millisecond)
	if strings.TrimSpace(t.Payload) == "" {
		return "", fmt.Errorf("task %d has an empty payload", t.ID)
	}
	upper := strings.ToUpper(t.Payload)
	vowels := 0
	for _, c := range upper {
		if strings.ContainsRune("AEIOU", c) {
			vowels++
		}
	}
	return fmt.Sprintf("%s (chars=%d, vowels=%d)", upper, len(upper), vowels), nil
}

func worker(name string, queue *TaskQueue, store *ResultStore, failures *int, failMu *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("[INFO   ] %s started", name)
	done := 0
	defer func() { log.Printf("[INFO   ] %s completed (%d tasks)", name, done) }()

	for {
		task, err := queue.GetTask()
		if errors.Is(err, ErrQueueClosed) {
			return
		}
		output, err := process(task)
		if err == nil {
			err = store.Save(Result{task.ID, name, output})
		}
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

func run(workerCount int, outPath string) error {
	file, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("cannot open output file: %w", err)
	}
	defer file.Close()

	store := &ResultStore{writer: bufio.NewWriter(file)}
	payloads := []string{"alpha", "bravo", "charlie", "delta", "", "echo", "foxtrot",
		"golf", "hotel", "india", "", "juliet"}

	queue := NewTaskQueue(len(payloads))
	var wg sync.WaitGroup
	var failMu sync.Mutex
	failures := 0

	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go worker(fmt.Sprintf("worker-%d", i), queue, store, &failures, &failMu, &wg)
	}
	for i, p := range payloads {
		queue.AddTask(Task{i + 1, p})
	}
	queue.Close()
	wg.Wait()

	if err := store.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush output file: %w", err)
	}
	log.Printf("[INFO   ] done: %d tasks, %d succeeded, %d failed, results in %s",
		len(payloads), store.Count(), failures, outPath)
	return nil
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	workerCount := 4
	if len(os.Args) > 1 {
		n, err := strconv.Atoi(os.Args[1])
		if err != nil || n < 1 {
			log.Fatalf("[ERROR  ] invalid worker count %q", os.Args[1])
		}
		workerCount = n
	}
	if err := run(workerCount, "results_go.txt"); err != nil {
		log.Fatalf("[ERROR  ] %v", err)
	}
}
