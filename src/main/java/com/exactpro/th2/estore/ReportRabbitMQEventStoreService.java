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

import static com.exactpro.th2.estore.ProtoUtil.toCradleEvent;
import static com.exactpro.th2.estore.ProtoUtil.toCradleEventID;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventBatchOrBuilder;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.google.protobuf.MessageOrBuilder;

public class ReportRabbitMQEventStoreService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReportRabbitMQEventStoreService.class);
    private static final String[] ATTRIBUTES = {QueueAttribute.SUBSCRIBE.toString(), QueueAttribute.EVENT.toString()};
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Map<CompletableFuture<?>, MessageOrBuilder> futuresToComplete = new ConcurrentHashMap<>();
    private final MessageRouter<EventBatch> router;
    private final CradleStorage cradleStorage;
    private SubscriberMonitor monitor;

    public ReportRabbitMQEventStoreService(@NotNull MessageRouter<EventBatch> router, @NotNull CradleManager cradleManager) {
        this.router = requireNonNull(router, "Message router can't be null");
        cradleStorage = requireNonNull(cradleManager.getStorage(), "Cradle storage can't be null");
    }

    public void start() {
        if (monitor == null) {
            monitor = router.subscribeAll((tag, delivery) -> {
                try {
                    handle(delivery);
                } catch (Exception e) {
                    LOGGER.warn("Cannot handle delivery from consumer = {}", tag, e);
                }
            }, ATTRIBUTES);
            if (monitor == null) {
                LOGGER.error("Cannot find queues for subscribe");
                throw new RuntimeException("Cannot find queues for subscribe");
            }
            LOGGER.info("RabbitMQ subscribing was successful");
        }
    }

    public void handle(EventBatch eventBatch) {
        try {
            List<Event> events = eventBatch.getEventsList();
            LOGGER.trace("Received EventBatch with {} events, batch hash {}", events.size(), eventBatch.hashCode());
            if (events.isEmpty()) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Skipped empty event batch " + shortDebugString(eventBatch));
                }
                return;
            }

            if (events.size() == 1) {
                if (eventBatch.hasParentEventId()) {
                    storeEventBatch(eventBatch);
                } else {
                    storeEvent(events.get(0));
                }
            } else { // events.size() > 1
                if (eventBatch.hasParentEventId()) {
                    storeEventBatch(eventBatch);
                } else {
                    for (Event event : events) {
                        storeEvent(event);
                    }
                }
            }
            LOGGER.trace("Number of futures: {}", futuresToComplete.size());
            LOGGER.trace("Finished handling EventBatch with {} events, batch hash {}", events.size(), eventBatch.hashCode());

        } catch (CradleStorageException | IOException e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Failed to store event batch '{}'", shortDebugString(eventBatch), e);
            }
            throw new RuntimeException("Failed to store event batch", e);
        }

    }

    public void dispose() {
        if (monitor != null) {
            try {
                monitor.unsubscribe();
            } catch (Exception e) {
                LOGGER.error("Cannot unsubscribe from queues", e);
            }
        }

        LOGGER.info("Waiting for futures completion");
        try {
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

        LOGGER.info("Shutting down the executor");
        try {
            shutdownExecutor();
            LOGGER.info("Executor shutdown");
        } catch (Exception ex) {
            LOGGER.error("Cannot shutdown the executor", ex);
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void shutdownExecutor() throws InterruptedException {
        executor.shutdown();
        int timeout = 5;
        TimeUnit unit = TimeUnit.SECONDS;
        if (!executor.awaitTermination(timeout, unit)) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Executor is not terminated during the timeout: {} mls. Force shutdown", unit.toMillis(timeout));
            }
            List<Runnable> runnables = executor.shutdownNow();
            if (!runnables.isEmpty()) {
                LOGGER.warn("{} task(s) are not executed", runnables.size());
            }
        }
    }

    private void awaitFutures(Map<CompletableFuture<?>, MessageOrBuilder> futures, Collection<CompletableFuture<?>> futuresToRemove) {
        futures.forEach((future, object) -> {
            try {
                if (!future.isDone()) {
                    future.get(1, TimeUnit.SECONDS);
                }
            } catch (CancellationException | ExecutionException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("{} - storing {} object is failure", getClass().getSimpleName(), shortDebugString(object), e);
                }
            } catch (TimeoutException | InterruptedException e) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("{} - future related to {} object can't be completed", getClass().getSimpleName(), shortDebugString(object), e);
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

    private CompletableFuture<StoredTestEventId> storeEvent(Event protoEvent) throws IOException, CradleStorageException {
        StoredTestEventSingle cradleEventSingle = cradleStorage.getObjectsFactory().createTestEvent(toCradleEvent(protoEvent));

        LOGGER.debug("Storing event with parentId {}", protoEvent.getId());
        CompletableFuture<Void> result = cradleStorage.storeTestEventAsync(cradleEventSingle)
                .thenRun(() ->
                        LOGGER.debug("Stored single event id '{}' parent id '{}'",
                                cradleEventSingle.getId(), cradleEventSingle.getParentId())
                );
        futuresToComplete.put(result, protoEvent);
        return result
                .whenCompleteAsync((unused, ex) -> {
                    if (ex != null && LOGGER.isErrorEnabled()) {
                        LOGGER.error("Failed to store the event '{}'", shortDebugString(protoEvent), ex);
                    }
                    if (futuresToComplete.remove(result) == null) {
                        if (LOGGER.isWarnEnabled()) {
                            LOGGER.warn("Future related to the event '{}' is already removed from map", shortDebugString(protoEvent));
                        }
                    }
                }, executor)
                .thenApply(unused -> cradleEventSingle.getId());
    }

    private CompletableFuture<StoredTestEventId> storeEventBatch(EventBatch protoBatch) throws IOException, CradleStorageException {
        StoredTestEventBatch cradleBatch = toCradleBatch(protoBatch);

        LOGGER.debug("Storing batch with parentId {}", protoBatch.getParentEventId());
        CompletableFuture<Void> result = cradleStorage.storeTestEventAsync(cradleBatch)
                .thenRun(() -> LOGGER.debug("Stored batch id '{}' parent id '{}' size '{}'",
                        cradleBatch.getId(), cradleBatch.getParentId(), cradleBatch.getTestEventsCount()));
        futuresToComplete.put(result, protoBatch);
        return result
                .whenCompleteAsync((unused, ex) -> {
                    if (ex != null && LOGGER.isErrorEnabled()) {
                        LOGGER.error("Failed to store the event batch '{}'", shortDebugString(protoBatch), ex);
                    }
                    if (futuresToComplete.remove(result) == null) {
                        if (LOGGER.isWarnEnabled()) {
                            LOGGER.warn("Future related to the batch '{}' is already removed from map", shortDebugString(protoBatch));
                        }
                    }
                }, executor)
                .thenApply(unused -> cradleBatch.getId());
    }

    private StoredTestEventBatch toCradleBatch(EventBatchOrBuilder protoEventBatch) throws CradleStorageException {
        StoredTestEventBatch cradleEventsBatch = cradleStorage.getObjectsFactory().createTestEventBatch(TestEventBatchToStore.builder()
                .parentId(toCradleEventID(protoEventBatch.getParentEventId()))
                .build());
        for (Event protoEvent : protoEventBatch.getEventsList()) {
            cradleEventsBatch.addTestEvent(toCradleEvent(protoEvent));
        }
        return cradleEventsBatch;
    }
}
