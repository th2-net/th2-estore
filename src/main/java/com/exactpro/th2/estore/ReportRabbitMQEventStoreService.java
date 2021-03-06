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
import java.util.List;
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
import com.exactpro.th2.store.common.AbstractStorage;
import com.exactpro.th2.store.common.utils.ProtoUtil;

public class ReportRabbitMQEventStoreService extends AbstractStorage<EventBatch> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReportRabbitMQEventStoreService.class);

    public ReportRabbitMQEventStoreService(MessageRouter<EventBatch> router, @NotNull CradleManager cradleManager) {
        super(router, cradleManager);
    }

    @Override
    protected String[] getAttributes() {
        return new String[]{"subscribe", "event"};
    }

    @Override
    public void handle(EventBatch eventBatch) {
        try {
            List<Event> events = eventBatch.getEventsList();
            if (events.isEmpty()) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Skipped empty event batch " + shortDebugString(eventBatch));
                }
            } else if (events.size() == 1) {
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

    private StoredTestEventId storeEvent(Event protoEvent) throws IOException, CradleStorageException {
        StoredTestEventSingle cradleEventSingle = cradleStorage.getObjectsFactory().createTestEvent(toCradleEvent(protoEvent));

        cradleStorage.storeTestEvent(cradleEventSingle);
        LOGGER.debug("Stored single event id '{}' parent id '{}'",
                cradleEventSingle.getId(), cradleEventSingle.getParentId());

        storeAttachedMessages(null, protoEvent);

        return cradleEventSingle.getId();
    }

    private StoredTestEventId storeEventBatch(EventBatch protoBatch) throws IOException, CradleStorageException {
        StoredTestEventBatch cradleBatch = toCradleBatch(protoBatch);
        cradleStorage.storeTestEvent(cradleBatch);
        LOGGER.debug("Stored batch id '{}' parent id '{}' size '{}'",
                cradleBatch.getId(), cradleBatch.getParentId(), cradleBatch.getTestEventsCount());

        for (Event protoEvent : protoBatch.getEventsList()) {
            storeAttachedMessages(cradleBatch.getId(), protoEvent);
        }
        return cradleBatch.getId();
    }

    private void storeAttachedMessages(StoredTestEventId batchID, Event protoEvent) throws IOException {
        List<MessageID> attachedMessageIds = protoEvent.getAttachedMessageIdsList();
        if (!attachedMessageIds.isEmpty()) {
            List<StoredMessageId> messagesIds = attachedMessageIds.stream()
                    .map(ProtoUtil::toStoredMessageId)
                    .collect(Collectors.toList());

            cradleStorage.storeTestEventMessagesLink(
                    toCradleEventID(protoEvent.getId()),
                    batchID,
                    messagesIds);
            LOGGER.debug("Stored attached messages '{}' to event id '{}'", messagesIds, protoEvent.getId().getId());
        }
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
