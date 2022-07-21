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
    public static final int POLL_WAIT_TIMEOUT_MILLIS = 100;

    private final CradleStorage cradleStorage;
    private final BlockingQueue<StoredTestEvent> eventBatchQueue;
    private final Map<CompletableFuture<?>, StoredTestEvent> futuresToComplete = new ConcurrentHashMap<>();
    private volatile boolean stopped;

    public EventPersistor(@NotNull CradleManager cradleManager) {
        this.eventBatchQueue = new LinkedBlockingQueue<>();
        this.cradleStorage = requireNonNull(cradleManager.getStorage(), "Cradle storage can't be null");
    }


    public void start() {
        this.stopped = false;
        new Thread(this, THREAD_NAME_PREFIX + this.hashCode()).start();
    }


    @Override
    public void run() {
        while (!stopped) {
            try {
                StoredTestEvent event = eventBatchQueue.poll(POLL_WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (event != null)
                    try {
                        storeEvent(event);
                    } catch (IOException e) {
                        LOGGER.error("Exception storing event batch id '{}', rescheduling", event.getId(), e);
                        addToQueue(event);
                    }
            } catch (InterruptedException ie) {
                LOGGER.debug("Received InterruptedException. Ignoring");
            }
        }
    }


    @Override
    public void persist(StoredTestEvent data) {
        addToQueue(data);
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


    void storeEvent(StoredTestEvent event) throws IOException {
        CompletableFuture<Void> result = cradleStorage.storeTestEventAsync(event)
                .thenRun(() -> LOGGER.debug("Stored batch id '{}' parent id '{}'", event.getId(), event.getParentId()));

        futuresToComplete.put(result, event);
        result.whenCompleteAsync((unused, ex) -> {
            if (ex != null) {
                if (LOGGER.isErrorEnabled())
                    LOGGER.error("Failed to store the event batch id '{}', rescheduling", event.getId(), ex);
                addToQueue(event);
            }
            if (futuresToComplete.remove(result) == null) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Future related to the batch id '{}' is already removed from map", event.getId());
                }
            }
        });
    }


    private void addToQueue(StoredTestEvent event) {
        try {
            eventBatchQueue.put(event);
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException while adding event to queue");
        }
    }
}
