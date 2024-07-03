/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.estore;

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.errors.BookNotFoundException;
import com.exactpro.cradle.errors.PageNotFoundException;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.taskutils.BlockingScheduledRetryableTaskQueue;
import com.exactpro.th2.taskutils.FutureTracker;
import com.exactpro.th2.taskutils.RetryScheduler;
import com.exactpro.th2.taskutils.ScheduledRetryableTask;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.prometheus.client.Histogram;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.exactpro.th2.common.utils.ExecutorServiceUtilsKt.shutdownGracefully;
import static java.util.Objects.requireNonNull;

public class EventPersistor implements Runnable, Persistor<TestEventToStore>, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventPersistor.class);
    private static final String THREAD_NAME_PREFIX = "event-persistor-thread-";
    private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("event-persistor-%d").build();
    private final CradleStorage cradleStorage;
    private final BlockingScheduledRetryableTaskQueue<PersistenceTask> taskQueue;
    private final FutureTracker<Void> futures;
    private volatile boolean stopped;
    private final Object signal = new Object();
    private final int maxTaskRetries;

    private final EventPersistorMetrics<PersistenceTask> metrics;
    private final ScheduledExecutorService executor;
    private final ErrorCollector errorCollector;

    public EventPersistor(@NotNull ErrorCollector errorCollector,
                          @NotNull Configuration config,
                          @NotNull CradleStorage cradleStorage) {
        this(errorCollector, config, cradleStorage, (r) -> config.getRetryDelayBase() * 1_000_000 * (r + 1));
    }

    public EventPersistor(@NotNull ErrorCollector errorCollector,
                          @NotNull Configuration config,
                          @NotNull CradleStorage cradleStorage,
                          RetryScheduler scheduler) {
        this.errorCollector = requireNonNull(errorCollector, "Error collector can't be null");
        this.maxTaskRetries = config.getMaxRetryCount();
        this.cradleStorage = requireNonNull(cradleStorage, "Cradle storage can't be null");
        this.taskQueue = new BlockingScheduledRetryableTaskQueue<>(config.getMaxTaskCount(), config.getMaxTaskDataSize(), scheduler);
        this.futures = new FutureTracker<>();
        this.metrics = new EventPersistorMetrics<>(taskQueue);
        this.executor = Executors.newScheduledThreadPool(config.getProcessingThreads(), THREAD_FACTORY);
    }

    public void start() throws InterruptedException {
        this.stopped = false;
        synchronized (signal) {
            // FIXME: control resource
            new Thread(this, THREAD_NAME_PREFIX + this.hashCode()).start();
            signal.wait();
        }
    }

    @Override
    public void run() {
        synchronized (signal) {
            signal.notifyAll();
        }

        LOGGER.info("EventProcessor started. Maximum data size for tasks = {}, maximum number of tasks = {}",
                taskQueue.getMaxDataSize(), taskQueue.getMaxTaskCount());
        executor.scheduleWithFixedDelay(
                metrics::takeQueueMeasurements,
                0,
                1,
                TimeUnit.SECONDS
        );
        while (!stopped) {
            try {
                ScheduledRetryableTask<PersistenceTask> task = taskQueue.awaitScheduled();
                    try {
                        processTask(task);
                    } catch (Exception e) {
                        resolveTaskError(task, e);
                    }
            } catch (InterruptedException ie) {
                LOGGER.debug("Received InterruptedException. aborting");
                break;
            }
        }
    }

    private void logAndFail(ScheduledRetryableTask<PersistenceTask> task, String errorKey, String logMessage, Throwable e) {
        taskQueue.complete(task);
        metrics.registerAbortedPersistence();
        errorCollector.collect(errorKey);
        LOGGER.error(logMessage, e);
        task.getPayload().fail();
    }

    private void resolveTaskError(ScheduledRetryableTask<PersistenceTask> task, Throwable e) {
        if (e instanceof BookNotFoundException || e instanceof PageNotFoundException) {
            // If following exceptions were thrown there's no point in retrying
            String message = String.format("Can't retry after %s exception", e.getClass().getSimpleName());
            logAndFail(task, message, message, e);
        } else {
            logAndRetry(task, e);
        }
    }


    @Override
    public void persist(TestEventToStore event, Callback<TestEventToStore> callback) {
        metrics.takeQueueMeasurements();
        PersistenceTask task = new PersistenceTask(event, callback);
        taskQueue.submit(new ScheduledRetryableTask<>(System.nanoTime(), maxTaskRetries, getEventContentSize(event), task));
    }


    public long getEventContentSize(TestEventToStore event) {
        if (event.isSingle())
            return event.asSingle().getContent().length;
        else if (event.isBatch())
            return event.asBatch().getBatchSize();
        else
            throw new IllegalArgumentException(String.format("Unknown event class (%s)", event.getClass().getSimpleName()));
    }

    private int getEventCount(TestEventToStore event) {
        if (event.isSingle())
            return 1;
        else if (event.isBatch())
            return event.asBatch().getTestEventsCount();
        else
            throw new IllegalArgumentException(String.format("Unknown event class (%s)", event.getClass().getSimpleName()));
    }


    public void close () {

        LOGGER.info("Waiting for futures completion");
        try {
            stopped = true;
            futures.awaitRemaining();
            LOGGER.info("All waiting futures are completed");
        } catch (Exception ex) {
            errorCollector.collect(LOGGER, "Cannot await all futures are finished", ex);
        }
        shutdownGracefully(executor, 1, TimeUnit.MINUTES);
    }


    void processTask(ScheduledRetryableTask<PersistenceTask> task) throws IOException, CradleStorageException {

        final TestEventToStore event = task.getPayload().eventBatch;
        final Histogram.Timer timer = metrics.startMeasuringPersistenceLatency();
        CompletableFuture<Void> result = cradleStorage.storeTestEventAsync(event)
                .thenRun(() -> LOGGER.debug("Stored batch id '{}' parent id '{}'", event.getId(), event.getParentId()))
                .whenCompleteAsync(
                        (unused, ex) -> {
                            timer.observeDuration();
                            if (ex != null) {
                                resolveTaskError(task, ex);
                            } else {
                                taskQueue.complete(task);
                                metrics.updateEventMeasurements(getEventCount(event), task.getPayloadSize());
                                task.getPayload().complete();
                            }
                        },
                        executor
                );

        futures.track(result);
    }


    private void logAndRetry(ScheduledRetryableTask<PersistenceTask> task, Throwable e) {

        metrics.registerPersistenceFailure();
        int retriesDone = task.getRetriesDone() + 1;

        final PersistenceTask persistenceTask  = task.getPayload();
        final TestEventToStore eventBatch = persistenceTask.eventBatch;

        if (task.getRetriesLeft() > 0) {
            errorCollector.collect("Failed to store an event batch, rescheduling");
            LOGGER.error("Failed to store the event batch id '{}', {} retries left, rescheduling",
                    eventBatch.getId(),
                    task.getRetriesLeft(),
                    e);
            taskQueue.retry(task);
            metrics.registerPersistenceRetry(retriesDone);

        } else {
            logAndFail(task,
                    "Failed to store an event batch, aborting after several executions",
                    String.format("Failed to store the event batch id '%s', aborting after %d executions",
                            eventBatch.getId(),
                            retriesDone),
                    e);
        }
    }


    static class PersistenceTask {
        final TestEventToStore eventBatch;
        final Callback<TestEventToStore> callback;

        PersistenceTask(TestEventToStore eventBatch, Callback<TestEventToStore> callback) {
            this.eventBatch = eventBatch;
            this.callback = callback;
        }

        void complete () {
            if (callback != null)
                callback.onSuccess(eventBatch);
        }

        void fail() {
            if (callback != null)
                callback.onFail(eventBatch);
        }
    }
}
