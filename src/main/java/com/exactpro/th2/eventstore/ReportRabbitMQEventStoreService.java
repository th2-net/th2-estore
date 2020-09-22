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

package com.exactpro.th2.eventstore;

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
import com.exactpro.th2.infra.grpc.Event;
import com.exactpro.th2.infra.grpc.EventBatch;
import com.exactpro.th2.infra.grpc.MessageID;
import com.exactpro.th2.schema.message.MessageRouter;
import com.exactpro.th2.store.common.AbstractStorage;
import com.exactpro.th2.store.common.utils.ProtoUtil;

public class ReportRabbitMQEventStoreService extends AbstractStorage<EventBatch> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass() + "@" + this.hashCode());

    public ReportRabbitMQEventStoreService(MessageRouter<EventBatch> router, @NotNull CradleManager cradleManager) {
        super(router, cradleManager);
    }

    @Override
    protected String[] getAttributes() {
        return new String[]{"in"};
    }

    @Override
    public void handle(EventBatch delivery) {
        try {
            String parentId = null;
            boolean equals = true;
            for (Event event : delivery.getEventsList()) {


                if (!event.hasParentId()) {
                    equals = false;
                    break;
                }

                if (parentId == null) {
                    parentId = event.getParentId().getId();
                } else if (!parentId.equals(event.getParentId().getId())){
                    equals = false;
                    break;
                }
            }

            if (equals) {
                storeMessageBatch(delivery);
            } else {
                storeSingleMessages(delivery);
            }
        } catch (CradleStorageException | IOException e) {
            logger.error("Event batch storing '{}' failed", delivery, e);
            throw new RuntimeException("Event batch storing failed", e);
        }

    }

    private void storeSingleMessages(EventBatch delivery) throws IOException, CradleStorageException {
        for (Event event : delivery.getEventsList()) {
            StoredTestEventSingle cradleEventSingle = StoredTestEvent.newStoredTestEventSingle(ProtoUtil.toCradleEvent(event));

            getCradleManager().getStorage().storeTestEvent(cradleEventSingle);
            logger.debug("Stored single event id '{}' parent id '{}'",
                    cradleEventSingle.getId(), cradleEventSingle.getParentId());

            storeAttachedMessages(null, event);
        }
    }

    private void storeMessageBatch(EventBatch delivery) throws CradleStorageException, IOException {
        StoredTestEventBatch cradleBatch = ProtoUtil.toCradleBatch(delivery);
        getCradleManager().getStorage().storeTestEvent(cradleBatch);
        logger.debug("Stored batch id '{}' parent id '{}' size '{}'",
                cradleBatch.getId(), cradleBatch.getParentId(), cradleBatch.getTestEventsCount());

        for (Event event : delivery.getEventsList()) {
            storeAttachedMessages(cradleBatch.getId(), event);
        }

    }

    private void storeAttachedMessages(StoredTestEventId batchID, Event protoEvent) throws IOException {
        List<MessageID> attachedMessageIds = protoEvent.getAttachedMessageIdsList();
        if (!attachedMessageIds.isEmpty()) {
            List<StoredMessageId> messagesIds = attachedMessageIds.stream()
                    .map(ProtoUtil::toStoredMessageId)
                    .collect(Collectors.toList());

            getCradleManager().getStorage().storeTestEventMessagesLink(
                    ProtoUtil.toCradleEventID(protoEvent.getId()),
                    batchID,
                    messagesIds);
            logger.debug("Stored attached messages '{}' to event id '{}'", messagesIds, protoEvent.getId().getId());
        }
    }
}
