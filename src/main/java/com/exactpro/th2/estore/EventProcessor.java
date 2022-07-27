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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventBatchOrBuilder;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.google.protobuf.MessageOrBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

import static com.exactpro.th2.common.util.StorageUtils.toInstant;
import static com.exactpro.th2.estore.ProtoUtil.*;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.util.Objects.requireNonNull;

public class EventProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);
    private static final String[] ATTRIBUTES = {QueueAttribute.SUBSCRIBE.toString(), QueueAttribute.EVENT.toString()};
    private final Map<CompletableFuture<?>, MessageOrBuilder> futuresToComplete = new ConcurrentHashMap<>();
    private final MessageRouter<EventBatch> router;
    private final CradleStorage cradleStorage;
    private SubscriberMonitor monitor;
    private final Persistor<TestEventToStore> persistor;

    public EventProcessor(@NotNull MessageRouter<EventBatch> router,
                          @NotNull CradleManager cradleManager,
                          @NotNull Persistor<TestEventToStore> persistor) {
        this.router = requireNonNull(router, "Message router can't be null");
        this.cradleStorage = requireNonNull(cradleManager.getStorage(), "Cradle storage can't be null");
        this.persistor = persistor;
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
            if (monitor != null) {
                LOGGER.info("RabbitMQ subscribing was successful");
            } else {
                LOGGER.error("Cannot find queues for subscribe");
                throw new RuntimeException("Cannot find queues for subscribe");
            }
        }
    }

    public void handle(EventBatch eventBatch) {
        try {
            List<Event> events = eventBatch.getEventsList();
            if (events.isEmpty()) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Skipped empty event batch " + shortDebugString(eventBatch));
                }
                return;
            }

            if (eventBatch.hasParentEventId()) {
                storeEventBatch(eventBatch);
            } else {
                for (Event event : events) {
                    storeSingleEvent(event);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to store event batch '{}'", shortDebugString(eventBatch), e);
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

    private void storeSingleEvent(Event protoEvent) throws Exception {
        TestEventSingleToStore cradleEventSingle = toCradleEvent(protoEvent);
        persistor.persist(cradleEventSingle);
    }


    private void storeEventBatch(EventBatch protoBatch) throws Exception {

        TestEventBatchToStore cradleBatch = toCradleBatch(protoBatch);
        persistor.persist(cradleBatch);
    }

    private TestEventBatchToStore toCradleBatch(EventBatchOrBuilder protoEventBatch) throws CradleStorageException {
        TestEventBatchToStore cradleEventBatch = cradleStorage.getEntitiesFactory()
                .testEventBatchBuilder()
                .id(
                        new BookId(protoEventBatch.getParentEventId().getBookName()),
                        protoEventBatch.getParentEventId().getScope(),
                        toInstant(getMinStartTimestamp(protoEventBatch.getEventsList())),
                        UUID.randomUUID().toString()
                )
                .parentId(toCradleEventID(protoEventBatch.getParentEventId()))
                .build();
        for (Event protoEvent : protoEventBatch.getEventsList()) {
            cradleEventBatch.addTestEvent(toCradleEvent(protoEvent));
        }
        return cradleEventBatch;
    }
}
