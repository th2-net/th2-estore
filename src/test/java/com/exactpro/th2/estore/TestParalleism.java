/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleObjectsFactory;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.EventStatus;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.estore.EventStoreMain.Interrupter;
import com.exactpro.th2.estore.configuration.CustomConfiguration;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestParalleism {

    private final Random random = new Random();
    private CradleManager cradleManagerMock = mock(CradleManager.class);
    private CradleStorage storageMock = mock(CradleStorage.class);
    @SuppressWarnings("unchecked")
    private final MessageRouter<EventBatch> routerMock = mock(MessageRouter.class);

    private ReportRabbitMQEventStoreService eventStore;
    private final CradleObjectsFactory cradleObjectsFactory = spy(new CradleObjectsFactory(StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE, StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE));


    @BeforeEach
    void setUp() throws IOException {
        cradleManagerMock = mock(CradleManager.class);

        storageMock = mock(CradleStorage.class);
        when(storageMock.getObjectsFactory()).thenReturn(cradleObjectsFactory);
        when(cradleManagerMock.getStorage()).thenReturn(storageMock);

        doReturn(CompletableFuture.completedFuture(null)).when(storageMock).storeTestEventAsync(any());
        doReturn(CompletableFuture.completedFuture(null)).when(storageMock).storeTestEventMessagesLinkAsync(any(), any(), any());
    }

    @Test
    public void testLimitOfParallelExecution() {
        int parallelism = 2;
        Interrupter interrupter = mock(Interrupter.class);
        Semaphore semaphore = new Semaphore(0);

        doAnswer((invocation) -> {
            System.out.println("acquiring");
            semaphore.acquire();
            System.out.println("acquire");
            return CompletableFuture.completedFuture(null);
        }).when(storageMock).storeTestEventMessagesLinkAsync(any(), any(), any());


        CustomConfiguration configuration = new CustomConfiguration();
        configuration.setParallelism(parallelism);
        configuration.setTimeout(1);
        configuration.setTimeUnit(TimeUnit.NANOSECONDS);

        EventBatch eventBatch = deliveryOf(createEvent(null, "root", 1));
        eventStore = spy(new ReportRabbitMQEventStoreService(routerMock, cradleManagerMock, configuration, interrupter));
        try {
            for (int iteration = 0; iteration < parallelism; iteration++) {
                eventStore.handle(eventBatch);
                verify(interrupter, never()).interrupt();
            }

            System.out.println("handling");
            assertThrows(IllegalStateException.class, () -> eventStore.handle(eventBatch));
            System.out.println("handled");
            verify(interrupter, times(1)).interrupt();
            System.out.println("release");
            semaphore.release(parallelism);
        } finally {
            eventStore.dispose();
        }
    }

    private Event createEvent(@Nullable String parentId, String name, int numberOfMessages) {
        var eventBuilder = Event.newBuilder()
                .setId(createEventID(String.valueOf(random.nextLong())))
                .setStartTimestamp(createTimestamp())
                .setName(name + '-' + random.nextInt())
                .setType("type-" + random.nextInt())
                .setBody(ByteString.copyFrom("msg-" + random.nextInt(), Charset.defaultCharset()))
                .setStatus(EventStatus.forNumber(random.nextInt(2)));
        if (parentId != null) {
            eventBuilder.setParentId(createEventID(parentId));
        }
        for (int i = 0; i < numberOfMessages; i++) {
            eventBuilder.addAttachedMessageIds(createMessageId("session-alias-" + random.nextInt(),
                    Direction.forNumber(random.nextInt(2)), random.nextLong()));
        }

        return eventBuilder.setEndTimestamp(createTimestamp())
                .build();
    }

    @NotNull
    protected MessageID createMessageId(String session, Direction direction, long sequence) {
        return MessageID.newBuilder()
                .setDirection(direction)
                .setSequence(sequence)
                .setConnectionId(ConnectionID.newBuilder().setSessionAlias(session).build())
                .build();
    }

    @NotNull
    private static EventID createEventID(String parentId) {
        return EventID.newBuilder()
                .setId(parentId)
                .build();
    }

    protected Timestamp createTimestamp() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
    }

    private static EventBatch deliveryOf(@Nullable String parentId, Event... events) {
        var eventBatchBuilder = EventBatch.newBuilder()
                .addAllEvents(List.of(events));
        if (parentId != null) {
            eventBatchBuilder.setParentEventId(createEventID(parentId));
        }

        return eventBatchBuilder.build();
    }

    private static EventBatch deliveryOf(Event... events) {
        return deliveryOf(null, events);
    }

}