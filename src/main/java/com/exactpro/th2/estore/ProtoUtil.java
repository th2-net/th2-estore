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

import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.testevents.TestEventToStoreBuilder;
import com.exactpro.th2.common.grpc.*;
import com.exactpro.th2.common.util.StorageUtils;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;

public class ProtoUtil {
    public static StoredMessageId toStoredMessageId(MessageIDOrBuilder messageId) {
        return new StoredMessageId(messageId.getConnectionId().getSessionAlias(), toCradleDirection(messageId.getDirection()), messageId.getSequence());
    }

    private static TestEventToStore buildCradleEvent(EventOrBuilder protoEvent, byte[] content, boolean success) {
        TestEventToStoreBuilder builder = TestEventToStore.builder()
                .id(toCradleEventID(protoEvent.getId()))
                .name(protoEvent.getName())
                .type(protoEvent.getType())
                .success(success)
                .content(content)
                .messageIds(protoEvent.getAttachedMessageIdsList().stream()
                        .map(ProtoUtil::toStoredMessageId)
                        .collect(Collectors.toList())
                );
        if (protoEvent.hasParentId())
            builder.parentId(toCradleEventID(protoEvent.getParentId()));

        if (protoEvent.hasStartTimestamp())
            builder.startTimestamp(toInstant(protoEvent.getStartTimestamp()));

        if (protoEvent.hasEndTimestamp())
            builder.endTimestamp(toInstant(protoEvent.getEndTimestamp()));

        return builder.build();
    }


    public static TestEventToStore toCradleEvent(EventOrBuilder protoEvent) {
        return buildCradleEvent(protoEvent, protoEvent.getBody().toByteArray(), isSuccess(protoEvent.getStatus()));
    }

    public static TestEventToStore toFailedCradleEvent(EventOrBuilder protoEvent, String message) {
        return buildCradleEvent(protoEvent, message.getBytes(StandardCharsets.UTF_8), false);
    }

    public static String formatEvent(Event event) {
        StringBuilder builder = new StringBuilder("EVENT");
        return formatEvent(builder,event).toString();
    }

    private static StringBuilder formatEvent(StringBuilder builder, Event event) {

        return   builder.append("\n id: ").append(toCradleEventID(event.getId()))
                        .append("\n name: ").append(event.getName())
                        .append("\n type: ").append(event.getType())
                        .append("\n parent_id: ").append(applyNotNull(event.getParentId(), ProtoUtil::toCradleEventID))
                        .append("\n start: ").append(applyNotNull(event.getStartTimestamp(), StorageUtils::toInstant))
                        .append("\n end: ").append(applyNotNull(event.getEndTimestamp(), StorageUtils::toInstant))
                        .append("\n success: ").append(isSuccess(event.getStatus()));
    }

    public static String formatBatch(EventBatch batch) {

        StringBuilder builder = new StringBuilder("EVENT_BATCH")
                                    .append("\n event_count: ").append(batch.getEventsCount())
                                    .append("\n parent_id: ").append(applyNotNull(batch.getParentEventId(), ProtoUtil::toCradleEventID));

        if (batch.getEventsCount() > 0) {
            builder.append("\nEVENT[0]");
            formatEvent(builder, batch.getEvents(0));
        }

        return builder.toString();
    }

    public static StoredTestEventId toCradleEventID(EventIDOrBuilder protoEventID) {
        return new StoredTestEventId(String.valueOf(protoEventID.getId()));
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

    private static <T, R> R applyNotNull(T value, Function<T, R> function) {
        if (value == null)
            return null;
        else
            return function.apply(value);
    }
}
