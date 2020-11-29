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

import static com.exactpro.th2.store.common.utils.ProtoUtil.toCradleBatch;
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
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventSingle;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.store.common.AbstractStorage;
import com.exactpro.th2.store.common.utils.ProtoUtil;

public class ReportRabbitMQEventStoreService extends AbstractStorage<EventBatch> {

    private static final Logger logger = LoggerFactory.getLogger(ReportRabbitMQEventStoreService.class);

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
                if (logger.isWarnEnabled()) {
                    logger.warn("Sipped empty event batch " + shortDebugString(eventBatch));
                }
            } else if (events.size() == 1) {
                if (eventBatch.hasParentEventId()) {
                    storeEventBatch(getCradleManager(), eventBatch);
                } else {
                    storeEvent(getCradleManager(), events.get(0));
                }
            } else { // events.size() > 1
                if (eventBatch.hasParentEventId()) {
                    storeEventBatch(getCradleManager(), eventBatch);
                } else {
                    if (logger.isErrorEnabled()) {
                        logger.error("Batch should have parent id " + shortDebugString(eventBatch));
                    }
                }
            }
        } catch (CradleStorageException | IOException e) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to store event batch '{}'", shortDebugString(eventBatch), e);
            }
            throw new RuntimeException("Failed to store event batch", e);
        }

    }

    public static StoredTestEventId storeEvent(CradleManager cradleManager, Event protoEvent) throws IOException, CradleStorageException {
        StoredTestEventSingle cradleEventSingle = StoredTestEvent.newStoredTestEventSingle(ProtoUtil.toCradleEvent(protoEvent));

        cradleManager.getStorage().storeTestEvent(cradleEventSingle);
        logger.debug("Stored single event id '{}' parent id '{}'",
                cradleEventSingle.getId(), cradleEventSingle.getParentId());

        storeAttachedMessages(cradleManager, null, protoEvent);

        return cradleEventSingle.getId();
    }

    public static StoredTestEventId storeEventBatch(CradleManager cradleManager, EventBatch protoBatch) throws IOException, CradleStorageException {
        StoredTestEventBatch cradleBatch = toCradleBatch(protoBatch);
        cradleManager.getStorage().storeTestEvent(cradleBatch);
        logger.debug("Stored batch id '{}' parent id '{}' size '{}'",
                cradleBatch.getId(), cradleBatch.getParentId(), cradleBatch.getTestEventsCount());

        for (Event protoEvent : protoBatch.getEventsList()) {
            storeAttachedMessages(cradleManager, cradleBatch.getId(), protoEvent);
        }
        return cradleBatch.getId();
    }

    private static void storeAttachedMessages(CradleManager cradleManager, StoredTestEventId batchID, Event protoEvent) throws IOException {
        List<MessageID> attachedMessageIds = protoEvent.getAttachedMessageIdsList();
        if (!attachedMessageIds.isEmpty()) {
            List<StoredMessageId> messagesIds = attachedMessageIds.stream()
                    .map(ProtoUtil::toStoredMessageId)
                    .collect(Collectors.toList());

            cradleManager.getStorage().storeTestEventMessagesLink(
                    toCradleEventID(protoEvent.getId()),
                    batchID,
                    messagesIds);
            logger.debug("Stored attached messages '{}' to event id '{}'", messagesIds, protoEvent.getId().getId());
        }
    }
}
