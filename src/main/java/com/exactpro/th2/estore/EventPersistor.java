/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.th2.taskutils.BlockingScheduledRetryableTaskQueue;
import com.exactpro.th2.taskutils.FutureTracker;
import com.exactpro.th2.taskutils.RetryScheduler;
import com.exactpro.th2.taskutils.ScheduledRetryableTask;
import io.prometheus.client.Histogram;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class EventPersistor implements Runnable, Persistor<StoredTestEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventPersistor.class);
    private static final String THREAD_NAME_PREFIX = "event-persistor-thread-";

    private final CradleStorage cradleStorage;
    private final BlockingScheduledRetryableTaskQueue<PersistenceTask> taskQueue;
    private final FutureTracker<Void> futures;
    private volatile boolean stopped;
    private final Object signal = new Object();
    private final int maxTaskRetries;

    private final EventPersistorMetrics<PersistenceTask> metrics;
    private final ScheduledExecutorService samplerService;

    public EventPersistor(@NotNull Configuration config, @NotNull CradleManager cradleManager) {
        this(config, cradleManager, (r) -> config.getRetryDelayBase() * 1_000_000 * (r + 1));
    }

    public EventPersistor(@NotNull Configuration config, @NotNull CradleManager cradleManager, RetryScheduler scheduler) {
        this.maxTaskRetries = config.getMaxRetryCount();
        this.cradleStorage = requireNonNull(cradleManager.getStorage(), "Cradle storage can't be null");
        this.taskQueue = new BlockingScheduledRetryableTaskQueue<>(config.getMaxTaskCount(), config.getMaxTaskDataSize(), scheduler);
        this.futures = new FutureTracker<>();
        this.metrics = new EventPersistorMetrics<>(taskQueue);
        this.samplerService = Executors.newSingleThreadScheduledExecutor();
    }


    public void start() throws InterruptedException {
        this.stopped = false;
        synchronized (signal) {
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
        samplerService.scheduleWithFixedDelay(
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

    private void logAndFail(ScheduledRetryableTask<PersistenceTask> task, String logMessage, Throwable e) {
        taskQueue.complete(task);
        metrics.registerAbortedPersistence();
        LOGGER.error(logMessage, e);
        task.getPayload().fail();
    }

    private void resolveTaskError(ScheduledRetryableTask<PersistenceTask> task, Throwable e) {
        if (e instanceof IOException && e.getMessage().startsWith("Invalid test event")) {
            // If following exceptions were thrown there's no point in retrying
            logAndFail(task, String.format("Can't retry after %s exception", e.getClass().getSimpleName()), e);
        } else {
            logAndRetry(task, e);
        }
    }


    @Override
    public void persist(StoredTestEvent event, Callback<StoredTestEvent> callback) {
        metrics.takeQueueMeasurements();
        PersistenceTask task = new PersistenceTask(event, callback);
        taskQueue.submit(new ScheduledRetryableTask<>(System.nanoTime(), maxTaskRetries, getEventContentSize(event), task));
    }


    public long getEventContentSize(StoredTestEvent event) {
        if (event instanceof StoredTestEventSingle)
            return ((StoredTestEventSingle) event).getContent().length;
        else if (event instanceof StoredTestEventBatch)
            return ((StoredTestEventBatch) event).getBatchSize();
        else
            throw new IllegalArgumentException(String.format("Unknown event class (%s)", event.getClass().getSimpleName()));
    }

    private int getEventCount(StoredTestEvent event) {
        if (event instanceof StoredTestEventSingle)
            return 1;
        else if (event instanceof StoredTestEventBatch)
            return ((StoredTestEventBatch) event).getTestEventsCount();
        else
            throw new IllegalArgumentException(String.format("Unknown event class (%s)", event.getClass().getSimpleName()));
    }


    public void dispose() {

        LOGGER.info("Waiting for futures completion");
        try {
            stopped = true;
            futures.awaitRemaining();
            LOGGER.info("All waiting futures are completed");
        } catch (Exception ex) {
            LOGGER.error("Cannot await all futures are finished", ex);
        }
        try {
            samplerService.shutdown();
            samplerService.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
        }
    }


    void processTask(ScheduledRetryableTask<PersistenceTask> task) throws IOException {

        final StoredTestEvent event = task.getPayload().eventBatch;
        final Histogram.Timer timer = metrics.startMeasuringPersistenceLatency();
        CompletableFuture<Void> result = cradleStorage.storeTestEventAsync(event)
                .thenRun(() -> LOGGER.debug("Stored batch id '{}' parent id '{}'", event.getId(), event.getParentId()))
                .whenCompleteAsync((unused, ex) ->
                    {
                        timer.observeDuration();
                        if (ex != null) {
                            resolveTaskError(task, ex);
                        } else {
                            taskQueue.complete(task);
                            metrics.updateEventMeasurements(getEventCount(event), task.getPayloadSize());
                            task.getPayload().complete();
                        }
                    }
                );

        futures.track(result);
    }


    private void logAndRetry(ScheduledRetryableTask<PersistenceTask> task, Throwable e) {

        metrics.registerPersistenceFailure();
        int retriesDone = task.getRetriesDone() + 1;

        final PersistenceTask persistenceTask  = task.getPayload();
        final StoredTestEvent eventBatch = persistenceTask.eventBatch;

        if (task.getRetriesLeft() > 0) {

            LOGGER.error("Failed to store the event batch id '{}', {} retries left, rescheduling",
                    eventBatch.getId(),
                    task.getRetriesLeft(),
                    e);
            taskQueue.retry(task);
            metrics.registerPersistenceRetry(retriesDone);

        } else {
            logAndFail(task,
                    String.format("Failed to store the event batch id '%s', aborting after %d executions",
                            eventBatch.getId(),
                            retriesDone),
                    e);
        }
    }


    static class PersistenceTask {
        final StoredTestEvent eventBatch;
        final Callback<StoredTestEvent> callback;

        PersistenceTask(StoredTestEvent eventBatch, Callback<StoredTestEvent> callback) {
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
