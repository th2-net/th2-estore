/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.estore;

import java.util.Comparator;
import java.util.stream.Collectors;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventIDOrBuilder;
import com.exactpro.th2.common.grpc.EventOrBuilder;
import com.exactpro.th2.common.grpc.EventStatus;
import com.exactpro.th2.common.grpc.MessageIDOrBuilder;
import com.google.protobuf.TimestampOrBuilder;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;

public class ProtoUtil {
    public static Comparator<Event> EVENT_START_TIMESTAMP_COMPARATOR = Comparator
            .<Event>comparingLong(event -> event.getStartTimestamp().getSeconds())
            .thenComparingInt(event -> event.getStartTimestamp().getNanos());

    public static StoredMessageId toStoredMessageId(MessageIDOrBuilder messageId, TimestampOrBuilder timestamp) {
        return new StoredMessageId(
                new BookId(messageId.getBookName()),
                messageId.getConnectionId().getSessionAlias(),
                toCradleDirection(messageId.getDirection()),
                toInstant(timestamp),
                messageId.getSequence()
        );
    }

    public static TestEventSingleToStore toCradleEvent(EventOrBuilder protoEvent, TimestampOrBuilder parentTimestamp) throws CradleStorageException {
        TestEventSingleToStoreBuilder builder = TestEventToStore
                .singleBuilder()
                .id(toCradleEventID(protoEvent.getId(), protoEvent.getStartTimestamp()))
                .name(protoEvent.getName())
                .type(protoEvent.getType())
                .success(isSuccess(protoEvent.getStatus()))
                .messages(protoEvent.getAttachedMessageIdsList().stream()
                        .map(messageId -> toStoredMessageId(messageId, protoEvent.getStartTimestamp()))
                        .collect(Collectors.toSet()))
                .content(protoEvent.getBody().toByteArray());
        if (protoEvent.hasParentId()) {
            builder.parentId(toCradleEventID(protoEvent.getParentId(), parentTimestamp));
        }
        if (protoEvent.hasEndTimestamp()) {
            builder.endTimestamp(toInstant(protoEvent.getEndTimestamp()));
        }
        return builder.build();
    }

    public static StoredTestEventId toCradleEventID(EventIDOrBuilder protoEventID, TimestampOrBuilder startTimestamp) {
        return new StoredTestEventId(
                new BookId(protoEventID.getBookName()),
                protoEventID.getScope(),
                toInstant(startTimestamp),
                String.valueOf(protoEventID.getId())
        );
    }

    /**
     * Converts protobuf event status to success Gradle status
     */
    public static boolean isSuccess(EventStatus protoEventStatus) {
        switch (protoEventStatus) {
            case SUCCESS:
                return true;
            case FAILED:
                return false;
            default:
                throw new IllegalArgumentException("Unknown the event status '" + protoEventStatus + '\'');
        }
    }
}