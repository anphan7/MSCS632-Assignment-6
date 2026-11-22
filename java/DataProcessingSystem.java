import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Multi-threaded data processing system.
 * A producer fills a shared TaskQueue, a fixed pool of worker threads pulls
 * tasks from it, processes them, and saves results to a shared ResultStore
 * (an in-memory list plus an output file).
 */
public class DataProcessingSystem {

    // One shared logger; java.util.logging is thread-safe, so all workers can use it.
    private static final Logger LOG = Logger.getLogger("DataProcessing");

    // A unit of work. Records are immutable, so a Task can be passed between threads safely.
    record Task(int id, String payload) {}

    // The outcome of one processed task, including which worker handled it.
    record Result(int taskId, String worker, String output) {}

    // Checked exception for bad input, so callers are forced to handle processing failures.
    static class ProcessingException extends Exception {
        ProcessingException(String message) {
            super(message);
        }
    }

    /**
     * Shared, thread-safe task queue.
     * A ReentrantLock protects the deque, and the notEmpty condition lets
     * workers sleep (instead of busy-waiting) until a task is available.
     */
    static class TaskQueue {
        private final Deque<Task> tasks = new ArrayDeque<>();
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition notEmpty = lock.newCondition();
        // Set once the producer has added every task; tells workers no more work is coming.
        private boolean closed;

        // Adds a task and wakes one waiting worker.
        void addTask(Task task) {
            lock.lock();
            try {
                if (closed) {
                    throw new IllegalStateException("queue is closed");
                }
                tasks.addLast(task);
                notEmpty.signal();
            } finally {
                // Always release the lock, even if an exception was thrown, to prevent deadlock.
                lock.unlock();
            }
        }

        // Removes and returns the next task, blocking while the queue is empty but still open.
        // Throws NoSuchElementException when the queue is empty AND closed (no work left).
        Task getTask() throws InterruptedException {
            lock.lock();
            try {
                // A while loop (not if) re-checks the state after waking, guarding against spurious wake-ups.
                while (tasks.isEmpty() && !closed) {
                    notEmpty.await();
                }
                if (tasks.isEmpty()) {
                    throw new NoSuchElementException("queue is empty and closed");
                }
                return tasks.removeFirst();
            } finally {
                lock.unlock();
            }
        }

        // Marks the queue closed and wakes ALL waiting workers so every one of them can exit.
        void close() {
            lock.lock();
            try {
                closed = true;
                notEmpty.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Shared output resource: a synchronized in-memory list and a results file.
     */
    static class ResultStore {
        // synchronizedList makes each add() atomic across threads.
        private final List<Result> results = Collections.synchronizedList(new ArrayList<>());
        private final BufferedWriter writer;

        // Opening the file can fail, so the IOException is passed to the caller to handle.
        ResultStore(Path file) throws IOException {
            writer = Files.newBufferedWriter(file);
        }

        // Saves a result to the list and the file.
        void save(Result result) throws IOException {
            results.add(result);
            // BufferedWriter is not thread-safe; locking on it keeps each line whole (no interleaving).
            synchronized (writer) {
                writer.write(String.format("task=%d worker=%s result=%s%n",
                        result.taskId(), result.worker(), result.output()));
            }
        }

        List<Result> all() {
            return results;
        }

        // Flushes buffered output and releases the file handle.
        void close() throws IOException {
            synchronized (writer) {
                writer.close();
            }
        }
    }

    // Simulates computational work on one task.
    static String process(Task task) throws InterruptedException, ProcessingException {
        // Sleep 100-300 ms (varies by task id) to represent processing time.
        Thread.sleep(100 + (task.id() * 37) % 200);
        // Empty payloads are invalid input and raise a ProcessingException.
        if (task.payload().isBlank()) {
            throw new ProcessingException("task " + task.id() + " has an empty payload");
        }
        // The "work": upper-case the text and count its characters and vowels.
        String upper = task.payload().toUpperCase();
        int vowels = upper.replaceAll("[^AEIOU]", "").length();
        return upper + " (chars=" + upper.length() + ", vowels=" + vowels + ")";
    }

    // Builds the worker loop that each pool thread runs until the queue is drained.
    static Runnable worker(String name, TaskQueue queue, ResultStore store,
                           AtomicInteger failures) {
        return () -> {
            LOG.info(name + " started");
            int done = 0;
            while (true) {
                Task task;
                // Step 1: get the next task from the shared queue.
                try {
                    task = queue.getTask();
                } catch (NoSuchElementException e) {
                    // Queue is empty and closed: normal, safe termination of this worker.
                    break;
                } catch (InterruptedException e) {
                    LOG.warning(name + " interrupted while waiting for a task");
                    // Restore the interrupt flag so the executor knows this thread was interrupted.
                    Thread.currentThread().interrupt();
                    break;
                }
                // Step 2: process the task and save the result.
                try {
                    String output = process(task);
                    store.save(new Result(task.id(), name, output));
                    done++;
                    LOG.info(name + " processed task " + task.id());
                } catch (ProcessingException e) {
                    // Bad input: log and count it, then keep going with the next task.
                    failures.incrementAndGet();
                    LOG.severe(name + " error: " + e.getMessage());
                } catch (IOException e) {
                    // File write failed: log and count it, but don't kill the worker.
                    failures.incrementAndGet();
                    LOG.severe(name + " failed to write task " + task.id() + ": " + e.getMessage());
                } catch (InterruptedException e) {
                    LOG.warning(name + " interrupted while processing task " + task.id());
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            LOG.info(name + " completed (" + done + " tasks)");
        };
    }

    public static void main(String[] args) {
        configureLogging();

        // Optional command-line argument: number of worker threads (default 4).
        int workerCount = 4;
        try {
            if (args.length > 0) {
                workerCount = Integer.parseInt(args[0]);
            }
            if (workerCount < 1) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            LOG.severe("invalid worker count \"" + args[0] + "\"");
            return;
        }

        Path outFile = Path.of("results_java.txt");
        // Sample data; the two empty strings deliberately trigger the error-handling path.
        String[] payloads = {"alpha", "bravo", "charlie", "delta", "", "echo", "foxtrot",
                "golf", "hotel", "india", "", "juliet"};

        TaskQueue queue = new TaskQueue();
        // AtomicInteger lets several threads increment the failure count without a lock.
        AtomicInteger failures = new AtomicInteger();
        ResultStore store;
        try {
            store = new ResultStore(outFile);
        } catch (IOException e) {
            LOG.severe("cannot open output file: " + e.getMessage());
            return;
        }

        // Executor manages the worker threads instead of creating Thread objects by hand.
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        for (int i = 1; i <= workerCount; i++) {
            pool.submit(worker("worker-" + i, queue, store, failures));
        }

        // Producer: add every task, then close the queue so workers know when to stop.
        for (int i = 0; i < payloads.length; i++) {
            queue.addTask(new Task(i + 1, payloads[i]));
        }
        queue.close();

        // Accept no new work, then wait for the workers to finish the queued tasks.
        pool.shutdown();
        try {
            if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warning("workers did not finish in time, forcing shutdown");
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            // Always close the file so buffered results are flushed to disk.
            try {
                store.close();
            } catch (IOException e) {
                LOG.severe("failed to close output file: " + e.getMessage());
            }
        }

        LOG.info(String.format("done: %d tasks, %d succeeded, %d failed, results in %s",
                payloads.length, store.all().size(), failures.get(), outFile));
    }

    // Replaces the default two-line log format with "time [LEVEL] message".
    private static void configureLogging() {
        LOG.setUseParentHandlers(false);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord r) {
                return String.format("%tT.%<tL [%-7s] %s%n", r.getMillis(),
                        r.getLevel() == Level.SEVERE ? "ERROR" : r.getLevel().getName(),
                        r.getMessage());
            }
        });
        LOG.addHandler(handler);
    }
}
