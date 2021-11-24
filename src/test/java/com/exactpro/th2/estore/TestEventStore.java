/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.exactpro.th2.store.common.utils.ProtoUtil.toInstant;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleEntitiesFactory;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventSingle;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.EventStatus;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampOrBuilder;

public class TestEventStore {
    private static final String SCOPE = "";

    private final Random random = new Random();
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
        eventStore.handle(deliveryOf(bookName(1)));
        verify(storageMock, never()).storeTestEventAsync(any());
    }

    @Test
    @DisplayName("root event without message")
    public void testRootEventDelivery() throws IOException, CradleStorageException {
        String bookName = bookName(random.nextInt());
        Event first = createEvent(bookName, "root");
        eventStore.handle(deliveryOf(bookName, first));

        verify(cradleEntitiesFactory, never()).testEventBatchBuilder();

        ArgumentCaptor<TestEventSingleToStore> capture = ArgumentCaptor.forClass(TestEventSingleToStore.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());

        TestEventSingleToStore value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEvent(value, first, first.getStartTimestamp());
    }

    @Test
    @DisplayName("sub-event without message")
    public void testSubEventDelivery() throws IOException, CradleStorageException {
        String bookName = bookName(random.nextInt());
        Event first = createEvent(bookName, "root-id", "sub-event");
        eventStore.handle(deliveryOf(bookName, first));

        verify(cradleEntitiesFactory, never()).testEventBatchBuilder();

        ArgumentCaptor<TestEventSingleToStore> capture = ArgumentCaptor.forClass(TestEventSingleToStore.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());

        TestEventSingleToStore value = capture.getValue();
        assertNotNull(value, "Captured stored sub-event");
        assertStoredEvent(value, first, first.getStartTimestamp());
    }

    @Test
    @DisplayName("multiple sub-events without messages")
    public void testMultipleSubEventsDelivery() throws IOException, CradleStorageException {
        String bookName = bookName(random.nextInt());
        Event first = createEvent(bookName, "root-id", "sub-event-first");
        Event second = createEvent(bookName, "root-id", "sub-event-second");
        eventStore.handle(deliveryOf(bookName, first, second));

        verify(cradleEntitiesFactory, never()).testEventBatchBuilder();

        ArgumentCaptor<TestEventSingleToStore> capture = ArgumentCaptor.forClass(TestEventSingleToStore.class);
        verify(storageMock, times(2)).storeTestEventAsync(capture.capture());

        TestEventSingleToStore value = capture.getAllValues().get(0);
        assertNotNull(value, "Captured first stored event");
        assertStoredEvent(value, first, first.getStartTimestamp());

        value = capture.getAllValues().get(1);
        assertNotNull(value, "Captured second stored event");
        assertStoredEvent(value, second, second.getStartTimestamp());
    }

    @Test
    @DisplayName("Event batch with two events without messages")
    public void testEventsBatchDelivery() throws IOException, CradleStorageException {
        String bookName = bookName(random.nextInt());
        String parentId = "root-id";
        Event first = createEvent(bookName, parentId, "sub-event-first");
        Event second = createEvent(bookName, parentId, "sub-event-second");
        eventStore.handle(deliveryOf(bookName, parentId, first, second));

        verify(cradleEntitiesFactory, times(1)).testEventBatchBuilder();

        ArgumentCaptor<TestEventBatchToStore> capture = ArgumentCaptor.forClass(TestEventBatchToStore.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());

        TestEventBatchToStore value = capture.getValue();
        assertNotNull(value, "Captured stored event batch");
        assertStoredEventBatch(
                bookName,
                SCOPE,
                first.getStartTimestamp(),
                parentId,
                value,
                first,
                second
        );
    }

    @Test
    @DisplayName("Root event with three messages")
    public void testRootEventWithMessagesDelivery() throws IOException, CradleStorageException {
        String bookName = bookName(random.nextInt());
        Event first = createEvent(bookName, "root", 3);
        eventStore.handle(deliveryOf(bookName, first));

        verify(cradleEntitiesFactory, never()).testEventBatchBuilder();

        ArgumentCaptor<TestEventSingleToStore> captureEvent = ArgumentCaptor.forClass(TestEventSingleToStore.class);
        verify(storageMock, times(1)).storeTestEventAsync(captureEvent.capture());

        TestEventSingleToStore capturedEvent = captureEvent.getValue();
        assertNotNull(capturedEvent, "Captured stored event");
        assertStoredEvent(capturedEvent, first, first.getStartTimestamp());

        assertEventAndStoredEvent(first, capturedEvent.getId(), capturedEvent.getMessages());
    }

    @Test
    @DisplayName("Event batch with two events and messages")
    public void testEventsBatchDeliveryWithMessages() throws IOException, CradleStorageException {
        String bookName = bookName(random.nextInt());
        String parentId = "root-id";
        Event first = createEvent(bookName, parentId, "sub-event-first", 2);
        Event second = createEvent(bookName, parentId, "sub-event-second", 2);
        eventStore.handle(deliveryOf(bookName, parentId, first, second));

        verify(cradleEntitiesFactory, times(1)).testEventBatchBuilder();

        ArgumentCaptor<TestEventBatchToStore> capture = ArgumentCaptor.forClass(TestEventBatchToStore.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());

        TestEventBatchToStore testEventBatchToStore = capture.getValue();
        assertNotNull(testEventBatchToStore, "Captured stored event batch");
        assertStoredEventBatch(bookName, SCOPE, first.getStartTimestamp(), parentId, testEventBatchToStore, first, second);

        List<BatchedStoredTestEvent> batchedStoredTestEvents = new ArrayList<>(testEventBatchToStore.getTestEvents());
        StoredTestEventId firstStoredEventId = batchedStoredTestEvents.get(0).getId();
        StoredTestEventId secondStoredEventId = batchedStoredTestEvents.get(1).getId();

        Map<StoredTestEventId, Set<StoredMessageId>> eventIdToMessageIds = testEventBatchToStore.getBatchMessages();
        assertEventAndStoredEvent(first, firstStoredEventId, eventIdToMessageIds.get(firstStoredEventId));
        assertEventAndStoredEvent(second, secondStoredEventId, eventIdToMessageIds.get(secondStoredEventId));
    }

    private void assertEventAndStoredEvent(Event event, StoredTestEventId capturedEventId, Collection<StoredMessageId> messageIds) {
        assertNotNull(capturedEventId);
        assertEquals(
                new StoredTestEventId(new BookId(event.getId().getBookName()), SCOPE, toInstant(event.getStartTimestamp()), event.getId().getId()),
                capturedEventId
        );
        assertAttachedMessages(event, messageIds);
    }

    private static void assertAttachedMessages(Event event, Collection<StoredMessageId> messageIds) {
        assertEquals(
                event.getAttachedMessageIdsList().stream().map(messageId -> ProtoUtil.toStoredMessageId(messageId, event.getStartTimestamp())).collect(Collectors.toSet()),
                new HashSet<>(messageIds)
        );
    }

    private static void assertStoredEventBatch(
            String expectedBookName,
            String expectedScope,
            Timestamp expectedTimestamp,
            String expectedId,
            TestEventBatchToStore actualBatch,
            Event... expectedEvents
    ) {
        assertEquals(
                new StoredTestEventId(new BookId(expectedBookName), expectedScope, toInstant(expectedTimestamp), expectedId),
                actualBatch.getParentId()
        );
        assertEquals(expectedEvents.length, actualBatch.getTestEvents().size(), "Event batch size");
        List<BatchedStoredTestEvent> actualEvents = new ArrayList<>(actualBatch.getTestEvents());
        for (int i = 0; i < expectedEvents.length; i++) {
            assertStoredEvent(actualEvents.get(i), expectedEvents[i], expectedEvents[0].getStartTimestamp());
        }
    }

    private static void assertStoredEvent(TestEventSingle actual, Event expected, Timestamp parentTimestamp) {
        assertEquals(
                new StoredTestEventId(new BookId(expected.getId().getBookName()), SCOPE, toInstant(expected.getStartTimestamp()), expected.getId().getId()),
                actual.getId()
        );
        if (expected.hasParentId()) {
            assertEquals(
                    new StoredTestEventId(new BookId(expected.getParentId().getBookName()), SCOPE, toInstant(parentTimestamp), expected.getParentId().getId()),
                    actual.getParentId()
            );
        } else {
            assertNull(actual.getParentId(), "Empty parent event id");
        }
        assertEquals(from(expected.getStartTimestamp()), actual.getStartTimestamp(), "Event start timestamp");
        assertEquals(from(expected.getEndTimestamp()), actual.getEndTimestamp(), "Event end timestamp");
        assertEquals(expected.getName(), actual.getName(), "Event name");
        assertEquals(expected.getType(), actual.getType(), "Event type");
        assertArrayEquals(expected.getBody().toByteArray(), actual.getContent(), "Event context");
        assertEquals(ProtoUtil.isSuccess(expected.getStatus()), actual.isSuccess(), "Event status");
        assertAttachedMessages(expected, actual.getMessages());
    }

    private Event createEvent(String bookName, String parentId, String name, int numberOfMessages) {
        var eventBuilder = Event.newBuilder()
                .setId(createEventID(String.valueOf(random.nextLong()), bookName))
                .setStartTimestamp(createTimestamp())
                .setName(name + '-' + random.nextInt())
                .setType("type-" + random.nextInt())
                .setBody(ByteString.copyFrom("msg-" + random.nextInt(), Charset.defaultCharset()))
                .setStatus(EventStatus.forNumber(random.nextInt(2)));
        if (parentId != null) {
            eventBuilder.setParentId(createEventID(parentId, bookName));
        }
        for (int i = 0; i < numberOfMessages; i++) {
            eventBuilder.addAttachedMessageIds(createMessageId(
                    "session-alias-" + random.nextInt(),
                    Direction.forNumber(random.nextInt(2)),
                    random.nextLong(),
                    bookName
            ));
        }
        return eventBuilder
                .setEndTimestamp(createTimestamp())
                .build();
    }

    private Event createEvent(String bookName, String name) {
        return createEvent(bookName, null, name, 0);
    }

    private Event createEvent(String bookName, String parentId, String name) {
        return createEvent(bookName, parentId, name, 0);
    }

    private Event createEvent(String bookName, String name, int numberOfMessages) {
        return createEvent(bookName, null, name, numberOfMessages);
    }

    @NotNull
    protected MessageID createMessageId(String sessionAlias, Direction direction, long sequence, String bookName) {
        return MessageID.newBuilder()
                .setConnectionId(ConnectionID.newBuilder().setSessionAlias(sessionAlias).build())
                .setDirection(direction)
                .setSequence(sequence)
                .setBookName(bookName)
                .build();
    }

    @NotNull
    private EventID createEventID(String id, String bookName) {
        return EventID.newBuilder()
                .setId(id)
                .setBookName(bookName)
                .build();
    }

    protected Timestamp createTimestamp() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
    }

    private EventBatch deliveryOf(String bookName, String parentId, Event... events) {
        var eventBatchBuilder = EventBatch.newBuilder()
                .addAllEvents(List.of(events));
        if (parentId != null) {
            eventBatchBuilder.setParentEventId(createEventID(parentId, bookName));
        }
        return eventBatchBuilder.build();
    }

    private EventBatch deliveryOf(String bookName, Event... events) {
        return deliveryOf(bookName, null, events);
    }

    private static Instant from(TimestampOrBuilder timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private static String bookName(int i) {
        return "book-name-" + i;
    }
}