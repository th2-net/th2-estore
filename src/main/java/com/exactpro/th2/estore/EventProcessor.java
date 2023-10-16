/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CradleEntitiesFactory;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventBatchOrBuilder;
import com.exactpro.th2.common.grpc.EventOrBuilder;
import com.exactpro.th2.common.schema.message.*;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation;
import com.exactpro.th2.common.util.StorageUtils;
import com.google.protobuf.TextFormat;
import io.prometheus.client.Histogram;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.protobuf.TextFormat.shortDebugString;
import static java.util.Objects.requireNonNull;

public class EventProcessor implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);
    private static final String[] ATTRIBUTES = {QueueAttribute.SUBSCRIBE.toString(), QueueAttribute.EVENT.toString()};
    private final ErrorCollector errorCollector;
    private final MessageRouter<EventBatch> router;
    private final CradleEntitiesFactory entitiesFactory;
    private final Persistor<TestEventToStore> persistor;
    private SubscriberMonitor monitor;
    private final EventProcessorMetrics metrics;

    public EventProcessor(@NotNull ErrorCollector errorCollector,
                          @NotNull MessageRouter<EventBatch> router,
                          @NotNull CradleEntitiesFactory entitiesFactory,
                          @NotNull Persistor<TestEventToStore> persistor) {
        this.errorCollector = requireNonNull(errorCollector, "Error collector can't be null");
        this.router = requireNonNull(router, "Message router can't be null");
        this.entitiesFactory = requireNonNull(entitiesFactory, "Cradle entity factory can't be null");
        this.persistor = persistor;
        this.metrics = new EventProcessorMetrics();
    }


    public void start() {

        if (monitor == null) {
            monitor = router.subscribeAllWithManualAck(
                    (deliveryMetadata, eventBatch, confirmation) -> process(eventBatch, confirmation),
                    ATTRIBUTES);

            if (monitor == null) {
                errorCollector.collect(LOGGER, "Cannot find queues for subscribe", null);
                throw new RuntimeException("Cannot find queues for subscribe");
            }
            LOGGER.info("RabbitMQ subscribing was successful");
        }
    }


    public void process(EventBatch eventBatch, Confirmation confirmation) {
        List<Event> events = eventBatch.getEventsList();
        if (events.isEmpty()) {
            if (LOGGER.isWarnEnabled())
                LOGGER.warn("Skipped empty event batch " + TextFormat.shortDebugString(eventBatch));
            confirm(confirmation);
        } else {
            if (eventBatch.hasParentEventId())
                storeEventBatch(eventBatch, confirmation);
            else
                storeSingleEvents(events, confirmation);
        }
    }


    @Override
    public void close() {
        if (monitor != null) {
            try {
                monitor.unsubscribe();
            } catch (Exception e) {
                errorCollector.collect(LOGGER, "Cannot unsubscribe from queues", e);
            }
        }
    }

    private void storeSingleEvents(List<Event> events, Confirmation confirmation) {

        Callback<TestEventToStore> persistorCallback = new ProcessorCallback(confirmation, events.size());

        events.forEach((event) -> {
            try {
                TestEventSingleToStore cradleEventSingle = toCradleEvent(event);
                persist(cradleEventSingle, persistorCallback);
            } catch (Exception e) {
                errorCollector.collect("Failed to process a single event");
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Failed to process single event '{}'", shortDebugString(event), e);
                }
                persistorCallback.onFail(null);
                metrics.registerFailure();
            }
        });
    }


    private void storeEventBatch(EventBatch eventBatch, Confirmation confirmation) {

        try {
            TestEventBatchToStore cradleBatch = toCradleBatch(eventBatch);
            persist(cradleBatch, new Callback<>() {
                @Override
                public void onSuccess(TestEventToStore data) {
                    confirm(confirmation);
                }

                @Override
                public void onFail(TestEventToStore data) {
                    reject(confirmation);
                }
            });

        } catch (Exception e) {
            errorCollector.collect("Failed to process an event batch");
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Failed to process event batch '{}'", shortDebugString(eventBatch), e);
            }
            reject(confirmation);
            metrics.registerFailure();
        }
    }


    private boolean confirm(Confirmation confirmation) {
        try {
            confirmation.confirm();
        } catch (Exception e) {
            errorCollector.collect(LOGGER, "Exception confirming message", e);
            return false;
        }
        return true;
    }


    private boolean reject(Confirmation confirmation) {
        try {
            confirmation.reject();
        } catch (Exception e) {
            errorCollector.collect(LOGGER, "Exception rejecting message", e);
            return false;
        }
        return true;
    }


    private void persist(TestEventToStore data, Callback<TestEventToStore> callback) throws Exception {
        try (Histogram.Timer ignored = metrics.startMeasuringPersistenceLatency()) {
            persistor.persist(data, callback);
        }
    }


    public TestEventSingleToStore toCradleEvent(EventOrBuilder protoEvent) throws CradleStorageException {
        TestEventSingleToStoreBuilder builder = entitiesFactory
                .testEventBuilder()
                .id(ProtoUtil.toCradleEventID(protoEvent.getId()))
                .name(protoEvent.getName())
                .type(protoEvent.getType())
                .success(ProtoUtil.isSuccess(protoEvent.getStatus()))
                .messages(protoEvent.getAttachedMessageIdsList().stream()
                        .map(ProtoUtil::toStoredMessageId)
                        .collect(Collectors.toSet()))
                .content(protoEvent.getBody().toByteArray());
        if (protoEvent.hasParentId()) {
            builder.parentId(ProtoUtil.toCradleEventID(protoEvent.getParentId()));
        }
        if (protoEvent.hasEndTimestamp()) {
            builder.endTimestamp(StorageUtils.toInstant(protoEvent.getEndTimestamp()));
        }
        return builder.build();
    }


    private TestEventBatchToStore toCradleBatch(EventBatchOrBuilder protoEventBatch) throws CradleStorageException {
        TestEventBatchToStore cradleEventBatch = entitiesFactory.testEventBatchBuilder()
                .id(
                        new BookId(protoEventBatch.getParentEventId().getBookName()),
                        protoEventBatch.getParentEventId().getScope(),
                        StorageUtils.toInstant(ProtoUtil.getMinStartTimestamp(protoEventBatch.getEventsList())),
                        Util.generateId()
                )
                .parentId(ProtoUtil.toCradleEventID(protoEventBatch.getParentEventId()))
                .build();
        for (Event protoEvent : protoEventBatch.getEventsList()) {
            cradleEventBatch.addTestEvent(toCradleEvent(protoEvent));
        }
        return cradleEventBatch;
    }

    private class ProcessorCallback implements Callback<TestEventToStore> {
        private final AtomicBoolean responded = new AtomicBoolean(false);
        private final Map<TestEventToStore, TestEventToStore> completed = new ConcurrentHashMap<>();
        private final Confirmation confirmation;
        private final int eventCount;

        public ProcessorCallback(Confirmation confirmation, int eventCount) {
            this.eventCount = eventCount;
            this.confirmation = confirmation;
        }

        @Override
        public void onSuccess(TestEventToStore persistedEvent) {
            completed.put(persistedEvent, persistedEvent);
            if (completed.size() == eventCount) {
                checkAndRespond(responded, () -> confirm(confirmation));
            }
        }

        @Override
        public void onFail(TestEventToStore persistedEvent) {
            checkAndRespond(responded, () -> reject(confirmation));
            if (persistedEvent != null) {
                completed.put(persistedEvent, persistedEvent);
            }
        }

        private void checkAndRespond(AtomicBoolean responded, Supplier<Boolean> confirmationFunction) {
            synchronized(responded) {
                if (!responded.get())
                    responded.set(confirmationFunction.get());
            }
        }
    }
}
