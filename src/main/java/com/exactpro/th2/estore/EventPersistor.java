/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.estore;

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventWithContent;
import com.exactpro.th2.taskutils.BlockingScheduledRetryableTaskQueue;
import com.exactpro.th2.taskutils.FutureTracker;
import com.exactpro.th2.taskutils.RetryScheduler;
import com.exactpro.th2.taskutils.ScheduledRetryableTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class EventPersistor implements Runnable, Persistor<StoredTestEvent>, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventPersistor.class);
    private static final String THREAD_NAME_PREFIX = "event-persistor-thread-";

    private final CradleStorage cradleStorage;
    private final BlockingScheduledRetryableTaskQueue<StoredTestEvent> taskQueue;
    private final FutureTracker<Void> futures;
    private volatile boolean stopped;
    private final Object signal = new Object();
    private final int maxTaskRetries;

    public EventPersistor(@NotNull Configuration config, @NotNull CradleStorage cradleStorage) {
        this(config, cradleStorage, (r) -> config.getRetryDelayBase() * 1_000_000 * (r + 1));
    }

    public EventPersistor(@NotNull Configuration config, @NotNull CradleStorage cradleStorager, RetryScheduler scheduler) {
        this.maxTaskRetries = config.getMaxRetryCount();
        this.cradleStorage = requireNonNull(cradleStorager, "Cradle storage can't be null");
        this.taskQueue = new BlockingScheduledRetryableTaskQueue<>(config.getMaxTaskCount(), config.getMaxTaskDataSize(), scheduler);
        this.futures = new FutureTracker<>();
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
        while (!stopped) {
            try {
                ScheduledRetryableTask<StoredTestEvent> task = taskQueue.awaitScheduled();
                    try {
                        processTask(task);
                    } catch (IOException e) {
                        logAndRetry(task, e);
                    }
            } catch (InterruptedException ie) {
                LOGGER.debug("Received InterruptedException. aborting");
                break;
            }
        }
    }


    @Override
    public void persist(StoredTestEvent event) {
        final long size;
        if (event instanceof StoredTestEventWithContent) {
            size = ((StoredTestEventWithContent) event).getContent().length;
        } else if (event instanceof StoredTestEventBatch) {
            size = ((StoredTestEventBatch) event).getBatchSize();
        } else
            throw new IllegalArgumentException(String.format("Unknown data class (%s)", event.getClass().getSimpleName()));

        taskQueue.submit(new ScheduledRetryableTask<>(System.nanoTime(), maxTaskRetries, size, event));
    }


    public void close () {

        LOGGER.info("Waiting for futures completion");
        try {
            stopped = true;
            futures.awaitRemaining();
            LOGGER.info("All waiting futures are completed");
        } catch (Exception ex) {
            LOGGER.error("Cannot await all futures are finished", ex);
        }
    }


    void processTask(ScheduledRetryableTask<StoredTestEvent> task) throws IOException {

        StoredTestEvent event = task.getPayload();
        CompletableFuture<Void> result = cradleStorage.storeTestEventAsync(event)
                .thenRun(() -> LOGGER.debug("Stored batch id '{}' parent id '{}'", event.getId(), event.getParentId()))
                .whenCompleteAsync((unused, ex) ->
                        {
                            if (ex != null)
                                logAndRetry(task, ex);
                            else
                                taskQueue.complete(task);
                        }
                );

        futures.track(result);
    }


    private void logAndRetry(ScheduledRetryableTask<StoredTestEvent> task, Throwable e) {
        if (task.getRetriesLeft() > 0) {
            LOGGER.error("Failed to store the event batch id '{}', {} retries left, rescheduling",
                    task.getPayload().getId(),
                    task.getRetriesLeft(),
                    e);
            taskQueue.retry(task);
        } else {
            taskQueue.complete(task);
            LOGGER.error("Failed to store the event batch id '{}', aborting after {} executions",
                    task.getPayload().getId(),
                    task.getRetriesDone() + 1,
                    e);
        }
    }
}
