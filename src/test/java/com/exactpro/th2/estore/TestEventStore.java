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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CradleEntitiesFactory;
import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.testevents.*;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.*;
import com.exactpro.th2.common.schema.message.MessageRouter;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.event.Event.start;
import static com.exactpro.th2.common.event.EventUtils.toEventID;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestEventStore {
    private static final String ROOT_ID = "root-id";
    private static final Random RANDOM = new Random();

    private final CradleManager cradleManagerMock = mock(CradleManager.class);
    private final CradleStorage storageMock = mock(CradleStorage.class);
    @SuppressWarnings("unchecked")
    private final MessageRouter<EventBatch> routerMock = mock(MessageRouter.class);

    private ReportRabbitMQEventStoreService eventStore;
    private CradleEntitiesFactory cradleEntitiesFactory;

    @BeforeEach
    void setUp() throws IOException, CradleStorageException {
        cradleEntitiesFactory = spy(new CradleEntitiesFactory(CradleStorage.DEFAULT_MAX_MESSAGE_BATCH_SIZE, CradleStorage.DEFAULT_MAX_MESSAGE_BATCH_SIZE));
        when(storageMock.getEntitiesFactory()).thenReturn(cradleEntitiesFactory);
        doReturn(CompletableFuture.completedFuture(null)).when(storageMock).storeTestEventAsync(any());

        when(cradleManagerMock.getStorage()).thenReturn(storageMock);
        eventStore = spy(new ReportRabbitMQEventStoreService(routerMock, cradleManagerMock));
    }

    @Test
    @DisplayName("Empty delivery is not stored")
    public void testEmptyDelivery() throws IOException, CradleStorageException {
        eventStore.handle(deliveryOf());
        verify(storageMock, never()).storeTestEventAsync(any());
    }

    @Test
    @DisplayName("root event without message")
    public void testRootEventDelivery() throws IOException, CradleStorageException {
        Event event = start().toProto(randomBookName(), randomScope());
        eventStore.handle(deliveryOf(event));

        verify(cradleEntitiesFactory, never()).testEventBatchBuilder();

        ArgumentCaptor<TestEventSingleToStore> capture = ArgumentCaptor.forClass(TestEventSingleToStore.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());

        TestEventSingleToStore capturedValue = capture.getValue();
        assertNotNull(capturedValue, "Captured stored root event");
        assertStoredEvent(event, capturedValue);
    }

    @Test
    @DisplayName("sub-event without message")
    public void testSubEventDelivery() throws IOException, CradleStorageException {
        Event event = start().name("sub-event").toProto(createRandomEventId());
        eventStore.handle(deliveryOf(event));

        verify(cradleEntitiesFactory, never()).testEventBatchBuilder();

        ArgumentCaptor<TestEventSingleToStore> capture = ArgumentCaptor.forClass(TestEventSingleToStore.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());

        TestEventSingleToStore capturedValue = capture.getValue();
        assertNotNull(capturedValue, "Captured stored sub-event");
        assertStoredEvent(event, capturedValue);
    }

    @Test
    @DisplayName("multiple sub-events without messages")
    public void testMultipleSubEventsDelivery() throws IOException, CradleStorageException {
        EventID parentId = createRandomEventId();
        Event first = start().name("sub-event-first").toProto(parentId);
        Event second = start().name("sub-event-second").toProto(parentId);
        eventStore.handle(deliveryOf(first, second));

        verify(cradleEntitiesFactory, never()).testEventBatchBuilder();

        ArgumentCaptor<TestEventSingleToStore> capture = ArgumentCaptor.forClass(TestEventSingleToStore.class);
        verify(storageMock, times(2)).storeTestEventAsync(capture.capture());
        List<TestEventSingleToStore> capturedValues = capture.getAllValues();

        TestEventSingleToStore capturedValue = capture.getAllValues().get(0);
        assertNotNull(capturedValue, "Captured first stored event");
        assertStoredEvent(first, capturedValues.get(0));

        capturedValue = capture.getAllValues().get(1);
        assertNotNull(capturedValue, "Captured second stored event");
        assertStoredEvent(second, capturedValues.get(1));
    }

    @Test
    @DisplayName("Event batch with two events without messages")
    public void testEventsBatchDelivery() throws IOException, CradleStorageException {
        EventID parentId = createRandomEventId();
        Event first = start().name("sub-event-first").toProto(parentId);
        Event second = start().name("sub-event-second").toProto(parentId);
        assertTestEventBatchToStore(parentId, first, second);
    }

    @Test
    @DisplayName("Event batch with two events in descending start time order without messages")
    public void testEventsBatchDeliveryDescendingStartTime() throws IOException, CradleStorageException {
        EventID parentId = createRandomEventId();
        Event second = start().name("sub-event-second").toProto(parentId);
        Event first = start().name("sub-event-first").toProto(parentId);
        assertTestEventBatchToStore(parentId, first, second);
    }

    @Test
    @DisplayName("Event batch with two events and messages")
    public void testEventsBatchDeliveryWithMessages() throws IOException, CradleStorageException {
        EventID parentId = createRandomEventId();
        assertTestEventBatchToStore(
                parentId,
                start()
                        .name("sub-event-first")
                        .messageID(createRandomMessageId(parentId))
                        .messageID(createRandomMessageId(parentId))
                        .toProto(parentId),
                start()
                        .name("sub-event-second")
                        .messageID(createRandomMessageId(parentId))
                        .messageID(createRandomMessageId(parentId))
                        .toProto(parentId)
        );
    }

    @Test
    @DisplayName("Root event with three messages")
    public void testRootEventWithMessagesDelivery() throws IOException, CradleStorageException {
        EventID parentId = createRandomEventId();
        Event first = start()
                .messageID(createRandomMessageId(parentId))
                .messageID(createRandomMessageId(parentId))
                .messageID(createRandomMessageId(parentId))
                .toProto(parentId);
        eventStore.handle(deliveryOf(first));

        verify(cradleEntitiesFactory, never()).testEventBatchBuilder();

        ArgumentCaptor<TestEventSingleToStore> captureEvent = ArgumentCaptor.forClass(TestEventSingleToStore.class);
        verify(storageMock, times(1)).storeTestEventAsync(captureEvent.capture());

        TestEventSingleToStore capturedValue = captureEvent.getValue();
        assertNotNull(capturedValue, "Captured stored event");
        assertStoredEvent(first, capturedValue);
    }

    private void assertTestEventBatchToStore(EventID parentId, Event first, Event second) throws IOException, CradleStorageException {
        eventStore.handle(deliveryOf(parentId, first, second));

        ArgumentCaptor<TestEventBatchToStore> capture = ArgumentCaptor.forClass(TestEventBatchToStore.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());

        TestEventBatchToStore testEventBatchToStore = capture.getValue();
        assertEquals(
                new StoredTestEventId(
                        new BookId(parentId.getBookName()),
                        parentId.getScope(),
                        toInstant(parentId.getStartTimestamp()),
                        parentId.getId()
                ),
                testEventBatchToStore.getParentId()
        );
        List<Event> expectedEvents = List.of(first, second);
        List<BatchedStoredTestEvent> actualEvents = new ArrayList<>(testEventBatchToStore.getTestEvents());
        assertEquals(expectedEvents.size(), actualEvents.size(), "Event batch size");
        for (int i = 0; i < expectedEvents.size(); i++) {
            assertStoredEvent(expectedEvents.get(i), actualEvents.get(i));
        }
    }

    private static void assertStoredEvent(Event expected, TestEventSingle actual) {
        assertNotNull(actual.getId());
        assertEquals(
                new StoredTestEventId(
                        new BookId(expected.getId().getBookName()),
                        expected.getId().getScope(),
                        toInstant(expected.getId().getStartTimestamp()),
                        expected.getId().getId()
                ),
                actual.getId()
        );
        if (expected.hasParentId()) {
            assertEquals(
                    new StoredTestEventId(
                            new BookId(expected.getParentId().getBookName()),
                            expected.getParentId().getScope(),
                            toInstant(expected.getParentId().getStartTimestamp()),
                            expected.getParentId().getId()
                    ),
                    actual.getParentId()
            );
        } else {
            assertNull(actual.getParentId(), "Empty parent event id");
        }
        assertEquals(toInstant(expected.getId().getStartTimestamp()), actual.getId().getStartTimestamp(), "Event start timestamp");
        assertEquals(toInstant(expected.getEndTimestamp()), actual.getEndTimestamp(), "Event end timestamp");
        assertEquals(expected.getName(), actual.getName(), "Event name");
        assertEquals(expected.getType(), actual.getType(), "Event type");
        assertArrayEquals(expected.getBody().toByteArray(), actual.getContent(), "Event context");
        assertEquals(ProtoUtil.isSuccess(expected.getStatus()), actual.isSuccess(), "Event status");

//        var actualIds = new ArrayList<>(actual.getMessages());
//        for (int i = 0; i < expected.getAttachedMessageIdsList().size(); i++) {
//            var expectedId = expected.getAttachedMessageIdsList().get(i);
//            var actualId = actualIds.get(i);
//            Assertions.assertEquals(expectedId.getTimestamp(), MessageUtils.toTimestamp(actualId.getTimestamp()));
//        }

        assertEquals(
                expected.getAttachedMessageIdsList().stream()
                        .map(ProtoUtil::toStoredMessageId)
                        .collect(Collectors.toSet()),
                new HashSet<>(actual.getMessages())
        );
    }

    private static EventID createRandomEventId() {
        return toEventID(Instant.now(), randomBookName(), randomScope(), ROOT_ID);
    }

    private static String randomBookName() {
        return "book-name-" + RANDOM.nextInt();
    }

    private static String randomScope() {
        return "scope-" + RANDOM.nextInt();
    }

    private static EventBatch deliveryOf(Event... events) {
        return deliveryOf(null, events);
    }

    private static EventBatch deliveryOf(EventID parentId, Event... events) {
        var eventBatchBuilder = EventBatch.newBuilder()
                .addAllEvents(List.of(events));
        if (parentId != null) {
            eventBatchBuilder.setParentEventId(parentId);
        }
        return eventBatchBuilder.build();
    }

    @NotNull
    private static MessageID createRandomMessageId(EventID parentId) {
        return MessageID.newBuilder()
                .setConnectionId(ConnectionID.newBuilder().setSessionAlias("session-alias-" + RANDOM.nextInt()).build())
                .setDirection(Direction.forNumber(RANDOM.nextInt(2)))
                .setSequence(RANDOM.nextLong())
                .setBookName(parentId.getBookName())
                .setTimestamp(com.exactpro.th2.common.message.MessageUtils.toTimestamp(Date.from(Instant.now())))
                .build();
    }
}
