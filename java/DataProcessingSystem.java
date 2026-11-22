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

public class DataProcessingSystem {

    private static final Logger LOG = Logger.getLogger("DataProcessing");

    record Task(int id, String payload) {}

    record Result(int taskId, String worker, String output) {}

    static class ProcessingException extends Exception {
        ProcessingException(String message) {
            super(message);
        }
    }

    static class TaskQueue {
        private final Deque<Task> tasks = new ArrayDeque<>();
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition notEmpty = lock.newCondition();
        private boolean closed;

        void addTask(Task task) {
            lock.lock();
            try {
                if (closed) {
                    throw new IllegalStateException("queue is closed");
                }
                tasks.addLast(task);
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }

        Task getTask() throws InterruptedException {
            lock.lock();
            try {
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

    static class ResultStore {
        private final List<Result> results = Collections.synchronizedList(new ArrayList<>());
        private final BufferedWriter writer;

        ResultStore(Path file) throws IOException {
            writer = Files.newBufferedWriter(file);
        }

        void save(Result result) throws IOException {
            results.add(result);
            synchronized (writer) {
                writer.write(String.format("task=%d worker=%s result=%s%n",
                        result.taskId(), result.worker(), result.output()));
            }
        }

        List<Result> all() {
            return results;
        }

        void close() throws IOException {
            synchronized (writer) {
                writer.close();
            }
        }
    }

    static String process(Task task) throws InterruptedException, ProcessingException {
        Thread.sleep(100 + (task.id() * 37) % 200);
        if (task.payload().isBlank()) {
            throw new ProcessingException("task " + task.id() + " has an empty payload");
        }
        String upper = task.payload().toUpperCase();
        int vowels = upper.replaceAll("[^AEIOU]", "").length();
        return upper + " (chars=" + upper.length() + ", vowels=" + vowels + ")";
    }

    static Runnable worker(String name, TaskQueue queue, ResultStore store,
                           AtomicInteger failures) {
        return () -> {
            LOG.info(name + " started");
            int done = 0;
            while (true) {
                Task task;
                try {
                    task = queue.getTask();
                } catch (NoSuchElementException e) {
                    break;
                } catch (InterruptedException e) {
                    LOG.warning(name + " interrupted while waiting for a task");
                    Thread.currentThread().interrupt();
                    break;
                }
                try {
                    String output = process(task);
                    store.save(new Result(task.id(), name, output));
                    done++;
                    LOG.info(name + " processed task " + task.id());
                } catch (ProcessingException e) {
                    failures.incrementAndGet();
                    LOG.severe(name + " error: " + e.getMessage());
                } catch (IOException e) {
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
        String[] payloads = {"alpha", "bravo", "charlie", "delta", "", "echo", "foxtrot",
                "golf", "hotel", "india", "", "juliet"};

        TaskQueue queue = new TaskQueue();
        AtomicInteger failures = new AtomicInteger();
        ResultStore store;
        try {
            store = new ResultStore(outFile);
        } catch (IOException e) {
            LOG.severe("cannot open output file: " + e.getMessage());
            return;
        }

        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        for (int i = 1; i <= workerCount; i++) {
            pool.submit(worker("worker-" + i, queue, store, failures));
        }
        for (int i = 0; i < payloads.length; i++) {
            queue.addTask(new Task(i + 1, payloads[i]));
        }
        queue.close();

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
            try {
                store.close();
            } catch (IOException e) {
                LOG.severe("failed to close output file: " + e.getMessage());
            }
        }

        LOG.info(String.format("done: %d tasks, %d succeeded, %d failed, results in %s",
                payloads.length, store.all().size(), failures.get(), outFile));
    }

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
