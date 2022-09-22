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

import com.exactpro.cradle.CradleObjectsFactory;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventBatchOrBuilder;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.google.protobuf.TextFormat;
import io.prometheus.client.Histogram;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.exactpro.th2.estore.ProtoUtil.toCradleEvent;
import static com.exactpro.th2.estore.ProtoUtil.toCradleEventID;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.util.Objects.requireNonNull;

public class EventProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);
    private static final String[] ATTRIBUTES = {QueueAttribute.SUBSCRIBE.toString(), QueueAttribute.EVENT.toString()};
    private final MessageRouter<EventBatch> router;
    private final CradleObjectsFactory objectsFactory;
    private final Persistor<StoredTestEvent> persistor;
    private SubscriberMonitor monitor;
    private final EventProcessorMetrics metrics;

    public EventProcessor(@NotNull MessageRouter<EventBatch> router,
                          @NotNull CradleObjectsFactory objectsFactory,
                          @NotNull Persistor<StoredTestEvent> persistor) {
        this.router = requireNonNull(router, "Message router can't be null");
        this.objectsFactory = requireNonNull(objectsFactory, "Cradle entities factory can't be null");
        this.persistor = persistor;
        this.metrics = new EventProcessorMetrics();
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
            if (events.isEmpty()) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Skipped empty event batch " + shortDebugString(eventBatch));
                }
                return;
            }

            if (eventBatch.hasParentEventId())
                storeEventBatch(eventBatch);
            else
                for (Event event : events)
                    storeSingleEvent(event);
        } catch (Exception e) {
            LOGGER.error("Failed to store event batch '{}'", TextFormat.shortDebugString(eventBatch), e);
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
    }


    private void storeSingleEvent(Event protoEvent) throws Exception {
        StoredTestEventSingle cradleEventSingle = objectsFactory.createTestEvent(toCradleEvent(protoEvent));
        persist(cradleEventSingle);
    }

    private void storeEventBatch(EventBatch protoBatch) throws Exception {
        StoredTestEventBatch cradleBatch = toCradleBatch(protoBatch);
        persist(cradleBatch);
    }


    private void persist(StoredTestEvent data) {
        Histogram.Timer timer = metrics.startMeasuringPersistenceLatency();
        try {
            persistor.persist(data);
        } catch (Exception e) {
            LOGGER.error("Persistence exception", e);
        } finally {
            timer.observeDuration();
        }
    }


    private StoredTestEventBatch toCradleBatch(EventBatchOrBuilder protoEventBatch) throws CradleStorageException {
        StoredTestEventBatch cradleEventsBatch = objectsFactory.createTestEventBatch(TestEventBatchToStore.builder()
                .parentId(toCradleEventID(protoEventBatch.getParentEventId()))
                .build());
        for (Event protoEvent : protoEventBatch.getEventsList()) {
            cradleEventsBatch.addTestEvent(toCradleEvent(protoEvent));
        }
        return cradleEventsBatch;
    }
}
