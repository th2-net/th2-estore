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

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.th2.estore.util.BlockingScheduledRetryableTaskQueue;
import com.exactpro.th2.estore.util.RetryScheduler;
import com.exactpro.th2.estore.util.ScheduledRetryableTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.*;

import static java.util.Objects.requireNonNull;

public class EventPersistor implements Runnable, Persistor<StoredTestEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventPersistor.class);
    private static final String THREAD_NAME_PREFIX = "event-persistor-thread-";

    private final CradleStorage cradleStorage;
    private final BlockingScheduledRetryableTaskQueue<StoredTestEvent> taskQueue;
    private final Map<CompletableFuture<?>, StoredTestEvent> futuresToComplete = new ConcurrentHashMap<>();
    private volatile boolean stopped;
    private Object signal = new Object();
    private final int maxTaskRetries;

    public EventPersistor(Configuration config, @NotNull CradleManager cradleManager) {
        this(config, cradleManager, (r) -> config.getRetryDelayBase() * 1_000_000 * (r + 1));
    }

    public EventPersistor(Configuration config, @NotNull CradleManager cradleManager, RetryScheduler scheduler) {
        this.cradleStorage = requireNonNull(cradleManager.getStorage(), "Cradle storage can't be null");
        this.taskQueue = new BlockingScheduledRetryableTaskQueue<>(config.getMaxTaskCount(), config.getMaxTaskDataSize(), scheduler);
        this.maxTaskRetries = config.getMaxRetryCount();
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
                        LOGGER.error("Exception storing event batch id '{}', rescheduling", task.getPayload().getId(), e);
                        taskQueue.retry(task);
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
        if (event instanceof StoredTestEventSingle) {
            size = ((StoredTestEventSingle) event).getContent().length;
        } else if (event instanceof StoredTestEventBatch) {
            size = ((StoredTestEventBatch) event).getBatchSize();
        } else
            throw new IllegalArgumentException(String.format("Unknown data class ({})", event.getClass().getSimpleName()));

        taskQueue.submit(new ScheduledRetryableTask<>(System.nanoTime(), maxTaskRetries, size, event));
    }


    public void dispose() {

        LOGGER.info("Waiting for futures completion");
        try {
            stopped = true;
            Collection<CompletableFuture<?>> futuresToRemove = new HashSet<>();
            while (!futuresToComplete.isEmpty() && !Thread.currentThread().isInterrupted()) {
                LOGGER.info("Wait for the completion of {} futures", futuresToComplete.size());
                futuresToRemove.clear();
                awaitFutures(futuresToComplete, futuresToRemove);
                futuresToComplete.keySet().removeAll(futuresToRemove);
            }
            LOGGER.info("All waiting futures are completed");
        } catch (Exception ex) {
            LOGGER.error("Cannot await all futures are finished", ex);
        }
    }


    private void awaitFutures(Map<CompletableFuture<?>, StoredTestEvent> futures, Collection<CompletableFuture<?>> futuresToRemove) {
        futures.forEach((future, event) -> {
            try {
                if (!future.isDone()) {
                    future.get(1, TimeUnit.SECONDS);
                }
            } catch (CancellationException | ExecutionException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("{} - storing event with id '{}' failed", getClass().getSimpleName(), event.getId(), e);
                }
            } catch (TimeoutException | InterruptedException e) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("{} - future related to event id '{}' can't be completed", getClass().getSimpleName(), event.getId(), e);
                }
                boolean mayInterruptIfRunning = e instanceof InterruptedException;
                future.cancel(mayInterruptIfRunning);

                if (mayInterruptIfRunning) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                futuresToRemove.add(future);
            }
        });
    }


    void processTask(ScheduledRetryableTask<StoredTestEvent> task) throws IOException {

        StoredTestEvent event = task.getPayload();
        CompletableFuture<Void> result = cradleStorage.storeTestEventAsync(event)
                .thenRun(() -> LOGGER.debug("Stored batch id '{}' parent id '{}'", event.getId(), event.getParentId()));

        futuresToComplete.put(result, event);
        result.whenCompleteAsync((unused, ex) -> {
            if (ex != null) {
                if (task.getRetriesLeft() > 0) {
                    LOGGER.error("Failed to store the event batch id '{}', rescheduling", event.getId(), ex);
                    taskQueue.retry(task);
                } else {
                    taskQueue.complete(task);
                    LOGGER.error("Failed to store the event batch id '{}', stopped retrying after {} executions",
                            event.getId(), task.getRetriesDone() + 1, ex);
                }
            } else
                taskQueue.complete(task);

            if (futuresToComplete.remove(result) == null) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Future related to the batch id '{}' is already removed from map", event.getId());
                }
            }
        });
    }
}
