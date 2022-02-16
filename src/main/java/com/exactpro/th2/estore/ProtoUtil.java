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

import java.util.stream.Collectors;

import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.testevents.TestEventToStoreBuilder;
import com.exactpro.th2.common.grpc.EventIDOrBuilder;
import com.exactpro.th2.common.grpc.EventOrBuilder;
import com.exactpro.th2.common.grpc.EventStatus;
import com.exactpro.th2.common.grpc.MessageIDOrBuilder;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;

public class ProtoUtil {
    public static StoredMessageId toStoredMessageId(MessageIDOrBuilder messageId) {
        return new StoredMessageId(messageId.getConnectionId().getSessionAlias(), toCradleDirection(messageId.getDirection()), messageId.getSequence());
    }

    public static TestEventToStore toCradleEvent(EventOrBuilder protoEvent) {
        TestEventToStoreBuilder builder = TestEventToStore.builder()
                .id(toCradleEventID(protoEvent.getId()))
                .name(protoEvent.getName())
                .type(protoEvent.getType())
                .success(isSuccess(protoEvent.getStatus()))
                .content(protoEvent.getBody().toByteArray())
                .messageIds(protoEvent.getAttachedMessageIdsList().stream()
                        .map(ProtoUtil::toStoredMessageId)
                        .collect(Collectors.toList())
                );
        if (protoEvent.hasParentId()) {
            builder.parentId(toCradleEventID(protoEvent.getParentId()));
        }
        if (protoEvent.hasStartTimestamp()) {
            builder.startTimestamp(toInstant(protoEvent.getStartTimestamp()));
        }
        if (protoEvent.hasEndTimestamp()) {
            builder.endTimestamp(toInstant(protoEvent.getEndTimestamp()));
        }
        return builder.build();
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
}
