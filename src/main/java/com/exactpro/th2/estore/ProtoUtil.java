/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventIDOrBuilder;
import com.exactpro.th2.common.grpc.EventStatus;
import com.exactpro.th2.common.grpc.MessageIDOrBuilder;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.util.StorageUtils;
import com.google.common.base.Strings;
import com.google.protobuf.Timestamp;

import java.util.Collection;
import java.util.Comparator;

public class ProtoUtil {
    private static final Comparator<Timestamp> TIMESTAMP_COMPARATOR = Comparator
            .comparingLong(Timestamp::getSeconds)
            .thenComparingInt(Timestamp::getNanos);

    public static StoredMessageId toStoredMessageId(MessageIDOrBuilder messageId) {
        return new StoredMessageId(
                new BookId(messageId.getBookName()),
                messageId.getConnectionId().getSessionAlias(),
                StorageUtils.toCradleDirection(messageId.getDirection()),
                StorageUtils.toInstant(messageId.getTimestamp()),
                messageId.getSequence()
        );
    }

    public static StoredTestEventId toCradleEventID(EventIDOrBuilder protoEventID) {
        String id = protoEventID.getId();
        if (Strings.isNullOrEmpty(id)) {
            throw new IllegalArgumentException("No unique identifier specified for event: " + MessageUtils.toJson(protoEventID));
        }

        return new StoredTestEventId(
                new BookId(protoEventID.getBookName()),
                protoEventID.getScope(),
                StorageUtils.toInstant(protoEventID.getStartTimestamp()),
                id
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

    public static Timestamp getMinStartTimestamp(Collection<Event> events) {
        if (events == null || events.isEmpty()) {
            throw new IllegalArgumentException("Events cannot be null or empty");
        }
        return events.stream()
                .map(event -> event.getId().getStartTimestamp())
                .min(TIMESTAMP_COMPARATOR)
                .get();
    }
}
