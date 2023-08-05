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
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation;
import com.exactpro.th2.common.schema.message.ManualConfirmationListener;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import io.prometheus.client.Histogram;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.exactpro.th2.estore.ProtoUtil.*;
import static com.exactpro.th2.estore.ProtoUtil.toCradleEvent;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.util.Objects.requireNonNull;

public class EventProcessor implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);
    private static final String[] ATTRIBUTES = {QueueAttribute.SUBSCRIBE.toString(), QueueAttribute.EVENT.toString()};

    private final MessageRouter<EventBatch> router;
    private SubscriberMonitor monitor;
    private final Persistor<StoredTestEvent> persistor;
    private final CradleStorage cradleStorage;
    private final EventProcessorMetrics metrics;
    private StoredTestEvent rootEvent;

    public EventProcessor(@NotNull MessageRouter<EventBatch> router,
                          @NotNull CradleManager cradleManager,
                          @NotNull Persistor<StoredTestEvent> persistor) {
        this.router = requireNonNull(router, "Message router can't be null");
        this.cradleStorage = requireNonNull(cradleManager.getStorage(), "Cradle storage can't be null");
        this.persistor = persistor;
        this.metrics = new EventProcessorMetrics();
    }

    public void start() throws Exception {

        rootEvent = createRootEvent();
        persistor.persist(rootEvent, null);

        if (monitor == null) {
            monitor = router.subscribeAllWithManualAck(new ManualConfirmationListener<>() {
                @Override
                public void handle(@NotNull String tag, EventBatch eventBatch, @NotNull Confirmation confirmation)  {
                    process(eventBatch, confirmation);
                }

                @Override
                public void onClose() {
                }
            }, ATTRIBUTES);

            if (monitor == null) {
                LOGGER.error("Cannot find queues for subscribe");
                throw new RuntimeException("Cannot find queues for subscribe");
            }
            LOGGER.info("RabbitMQ subscribing was successful");
        }
    }


    public void process(EventBatch eventBatch, Confirmation confirmation) {
        List<Event> events = eventBatch.getEventsList();
        if (events.isEmpty()) {
            if (LOGGER.isWarnEnabled())
                LOGGER.warn("Skipped empty event batch " + shortDebugString(eventBatch));
            confirm(confirmation);
            return;
        }

        if (eventBatch.hasParentEventId())
            storeEventBatch(eventBatch, confirmation);
        else
            storeSingleEvents(events, confirmation);
    }


    @Override
    public void close() {
        if (monitor != null) {
            try {
                monitor.unsubscribe();
            } catch (Exception e) {
                LOGGER.error("Cannot unsubscribe from queues", e);
            }
        }
    }


    private void checkAndRespond(AtomicBoolean responded, Supplier<Boolean> confirmationFunction) {
        synchronized(responded) {
            if (!responded.get())
                responded.set(confirmationFunction.get());
        }
    }


    private void storeSingleEvents(List<Event> events, Confirmation confirmation) {

        final int eventCount = events.size();
        final AtomicBoolean responded = new AtomicBoolean(false);
        final Map<StoredTestEvent, StoredTestEvent> completed = new ConcurrentHashMap<>();

        Callback<StoredTestEvent> persistorCallback = new Callback<>() {
            @Override
            public void onSuccess(StoredTestEvent persistedEvent) {
                completed.put(persistedEvent, persistedEvent);
                if (completed.size() == eventCount)
                    checkAndRespond(responded, () -> confirm(confirmation));
            }

            @Override
            public void onFail(StoredTestEvent persistedEvent) {
                checkAndRespond(responded, () -> reject(confirmation));
                if (persistedEvent != null)
                    completed.put(persistedEvent, persistedEvent);
            }
        };

        events.forEach((event) -> {
            try {
                StoredTestEventSingle cradleEvent = cradleStorage.getObjectsFactory().createTestEvent(toCradleEvent(event));
                persist(cradleEvent, persistorCallback);
            } catch (Exception e) {
                LOGGER.error("Failed to process single event'{}'", shortDebugString(event), e);
                persistorCallback.onFail(null);
                metrics.registerFailure();
            }
        });
    }


    private void storeEventBatch(EventBatch eventBatch, Confirmation confirmation) {

        try {
            StoredTestEventId parentId = toCradleEventID(eventBatch.getParentEventId());
            TestEventBatchToStore batchToStore = TestEventBatchToStore.builder().parentId(parentId).build();
            StoredTestEventBatch cradleBatch = cradleStorage.getObjectsFactory().createTestEventBatch(batchToStore);

            for (Event event : eventBatch.getEventsList())
                cradleBatch.addTestEvent(toCradleEvent(event));

            persist(cradleBatch, new Callback<>() {
                @Override
                public void onSuccess(StoredTestEvent data) {
                    confirm(confirmation);
                }

                @Override
                public void onFail(StoredTestEvent data) {
                    reject(confirmation);
                }
            });

        } catch (Exception e) {
            LOGGER.error("Failed to process event batch '{}'", shortDebugString(eventBatch), e);
            reject(confirmation);
            metrics.registerFailure();
        }
    }


    private static boolean confirm(Confirmation confirmation) {
        try {
            confirmation.confirm();
        } catch (Exception e) {
            LOGGER.error("Exception confirming message", e);
            return false;
        }
        return true;
    }


    private static boolean reject(Confirmation confirmation) {
        try {
            // no reject method in this impl
            confirmation.confirm();
        } catch (Exception e) {
            LOGGER.error("Exception rejecting message", e);
            return false;
        }
        return true;
    }


    private void persist(StoredTestEvent data, Callback<StoredTestEvent> callback) throws Exception {
        try (Histogram.Timer timer = metrics.startMeasuringPersistenceLatency();) {
            persistor.persist(data, callback);
        }
    }

    private StoredTestEventId createEventId() {
        return new StoredTestEventId(UUID.randomUUID().toString());
    }


    private StoredTestEvent createRootEvent() throws CradleStorageException {

        TestEventToStore event = TestEventToStore.builder()
                                                .id(createEventId())
                                                .startTimestamp(Instant.now())
                                                .name("estore")
                                                .type("Microservice")
                                                .success(true)
                                                .content("estore started".getBytes(StandardCharsets.UTF_8))
                                                .build();

        return cradleStorage.getObjectsFactory().createTestEvent(event);
    }

    private StoredTestEvent createFailureEvent(String eventBody) throws CradleStorageException {

        TestEventToStore event = TestEventToStore.builder()
                                                .id(createEventId())
                                                .startTimestamp(Instant.now())
                                                .name("cradle serialization error")
                                                .type("Error")
                                                .success(false)
                                                .content(eventBody.getBytes(StandardCharsets.UTF_8))
                                                .parentId(rootEvent.getId())
                                                .build();

        return cradleStorage.getObjectsFactory().createTestEvent(event);
    }
}
