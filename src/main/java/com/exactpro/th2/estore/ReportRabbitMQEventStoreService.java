/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import static com.exactpro.th2.store.common.utils.ProtoUtil.toCradleEvent;
import static com.exactpro.th2.store.common.utils.ProtoUtil.toCradleEventID;
import static com.google.protobuf.TextFormat.shortDebugString;

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
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventBatchOrBuilder;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.store.common.AbstractStorage;
import com.exactpro.th2.store.common.utils.ProtoUtil;
import com.google.protobuf.MessageOrBuilder;

public class ReportRabbitMQEventStoreService extends AbstractStorage<EventBatch> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReportRabbitMQEventStoreService.class);
    private static final String[] ATTRIBUTES = { QueueAttribute.SUBSCRIBE.toString(), QueueAttribute.EVENT.toString() };
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Map<CompletableFuture<?>, MessageOrBuilder> futuresToComplete = new ConcurrentHashMap<>();

    public ReportRabbitMQEventStoreService(MessageRouter<EventBatch> router, @NotNull CradleManager cradleManager) {
        super(router, cradleManager);
    }

    @Override
    protected String[] getAttributes() {
        return ATTRIBUTES;
    }

    @Override
    public void handle(EventBatch eventBatch) {
        try {
            List<Event> events = eventBatch.getEventsList();
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
        } catch (CradleStorageException | IOException e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Failed to store event batch '{}'", shortDebugString(eventBatch), e);
            }
            throw new RuntimeException("Failed to store event batch", e);
        }

    }

    @Override
    public void dispose() {
        super.dispose();

        logger.info("Waiting for futures completion");
        try {
            Collection<CompletableFuture<?>> futuresToRemove = new HashSet<>();
            while (!futuresToComplete.isEmpty() && !Thread.currentThread().isInterrupted()) {
                logger.info("Wait for the completion of {} futures", futuresToComplete.size());
                futuresToRemove.clear();
                awaitFutures(futuresToComplete, futuresToRemove);
                futuresToComplete.keySet().removeAll(futuresToRemove);
            }
            logger.info("All waiting futures are completed");
        } catch (Exception ex) {
            logger.error("Cannot await all futures are finished", ex);
        }

        logger.info("Shutting down the executor");
        try {
            shutdownExecutor();
            logger.info("Executor shutdown");
        } catch (Exception ex) {
            logger.error("Cannot shutdown the executor", ex);
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
            if (logger.isWarnEnabled()) {
                logger.warn("Executor is not terminated during the timeout: {} mls. Force shutdown", unit.toMillis(timeout));
            }
            List<Runnable> runnables = executor.shutdownNow();
            if (!runnables.isEmpty()) {
                logger.warn("{} task(s) are not executed", runnables.size());
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
                if (logger.isWarnEnabled()) {
                    logger.warn("{} - storing {} object is failure", getClass().getSimpleName(), shortDebugString(object), e);
                }
            } catch (TimeoutException | InterruptedException e) {
                if (logger.isErrorEnabled()) {
                    logger.error("{} - future related to {} object can't be completed", getClass().getSimpleName(), shortDebugString(object), e);
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

        CompletableFuture<Void> result = cradleStorage.storeTestEventAsync(cradleEventSingle)
                .thenRun(() ->
                        LOGGER.debug("Stored single event id '{}' parent id '{}'",
                                cradleEventSingle.getId(), cradleEventSingle.getParentId())
                )
                .thenComposeAsync(unused -> storeAttachedMessages(null, protoEvent), executor);
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

        CompletableFuture<Void> result = cradleStorage.storeTestEventAsync(cradleBatch)
                .thenRun(() -> LOGGER.debug("Stored batch id '{}' parent id '{}' size '{}'",
                        cradleBatch.getId(), cradleBatch.getParentId(), cradleBatch.getTestEventsCount()))
                .thenComposeAsync(unused -> CompletableFuture.allOf(
                        protoBatch.getEventsList().stream().map(it -> storeAttachedMessages(cradleBatch.getId(), it)).toArray(CompletableFuture[]::new)
                ), executor);
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

    private CompletableFuture<Void> storeAttachedMessages(StoredTestEventId batchID, Event protoEvent) {
        List<MessageID> attachedMessageIds = protoEvent.getAttachedMessageIdsList();
        if (!attachedMessageIds.isEmpty()) {
            List<StoredMessageId> messagesIds = attachedMessageIds.stream()
                    .map(ProtoUtil::toStoredMessageId)
                    .collect(Collectors.toList());

            return cradleStorage.storeTestEventMessagesLinkAsync(
                    toCradleEventID(protoEvent.getId()),
                    batchID,
                    messagesIds
            ).whenComplete((result, ex) -> {
                if (ex == null) {
                    LOGGER.debug("Stored attached messages '{}' to event id '{}'", messagesIds, protoEvent.getId().getId());
                } else {
                    LOGGER.error("Storing attached messages '{}' to event id '{}' failed", messagesIds, protoEvent.getId(), ex);
                }
            });
        }
        return CompletableFuture.completedFuture(null);
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
