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
import com.exactpro.cradle.testevents.*;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.schema.message.ConfirmationMessageListener;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation;
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
import java.util.UUID;

import static com.exactpro.th2.estore.ProtoUtil.*;
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
            monitor = router.subscribeAllWithManualAck(new ConfirmationMessageListener<>() {
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
        try {
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
                for (Event event : events)
                    storeSingleEvent(event, confirmation);
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error("Failed to store event batch '{}'", shortDebugString(eventBatch), e);
            confirm(confirmation);
            try {
                persist(createFailureEvent(formatBatch(eventBatch)), null);
            } catch (Exception pe) {
                LOGGER.error("Exception creating failure event", pe);
            }
        }
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


    private void storeSingleEvent(Event protoEvent, Confirmation confirmation) throws Exception {
        StoredTestEventSingle cradleEventSingle = cradleStorage.getObjectsFactory().createTestEvent(toCradleEvent(protoEvent));
        persist(cradleEventSingle, confirmation);
    }


    private void storeEventBatch(EventBatch protoBatch, Confirmation confirmation) throws Exception {
        TestEventBatchToStore batchToStore = TestEventBatchToStore.builder()
                .parentId(toCradleEventID(protoBatch.getParentEventId()))
                .build();

        StoredTestEventBatch cradleBatch = cradleStorage.getObjectsFactory().createTestEventBatch(batchToStore);

        for (Event protoEvent : protoBatch.getEventsList())
            cradleBatch.addTestEvent(toCradleEvent(protoEvent));

        persist(cradleBatch, confirmation);
    }

    private void confirm(Confirmation confirmation) {
        try {
            if (confirmation != null)
                confirmation.confirm();
        } catch (Exception e) {
            LOGGER.error("Exception sending acknowledgement", e);
        }
    }

    private void persist(StoredTestEvent data, Confirmation confirmation) {
        Histogram.Timer timer = metrics.startMeasuringPersistenceLatency();
        try {
            persistor.persist(data, (batch) -> confirm(confirmation));
        } catch (Exception e) {
            LOGGER.error("Persistence exception", e);
        } finally {
            timer.observeDuration();
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
