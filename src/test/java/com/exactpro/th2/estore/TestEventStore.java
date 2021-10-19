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

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleObjectsFactory;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventWithContent;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.EventStatus;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.store.common.utils.ProtoUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampOrBuilder;

public class TestEventStore {

    private final Random random = new Random();
    private final CradleManager cradleManagerMock = mock(CradleManager.class);
    private final CradleStorage storageMock = mock(CradleStorage.class);
    @SuppressWarnings("unchecked")
    private final MessageRouter<EventBatch> routerMock = mock(MessageRouter.class);

    private ReportRabbitMQEventStoreService eventStore;
    private CradleObjectsFactory cradleObjectsFactory;

    @BeforeEach
    void setUp() throws IOException {
        cradleObjectsFactory = spy(new CradleObjectsFactory(StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE, StoredMessageBatch.DEFAULT_MAX_BATCH_SIZE));
        when(storageMock.getObjectsFactory()).thenReturn(cradleObjectsFactory);
        doReturn(CompletableFuture.completedFuture(null)).when(storageMock).storeTestEventAsync(any());

        when(cradleManagerMock.getStorage()).thenReturn(storageMock);
        eventStore = spy(new ReportRabbitMQEventStoreService(routerMock, cradleManagerMock));
    }

    @Test
    @DisplayName("Empty delivery is not stored")
    public void testEmptyDelivery() throws IOException {
        eventStore.handle(deliveryOf());
        verify(storageMock, never()).storeTestEventAsync(any());
    }

    @Test
    @DisplayName("root event without message")
    public void testRootEventDelivery() throws IOException, CradleStorageException {
        Event first = createEvent("root");
        eventStore.handle(deliveryOf(first));

        verify(cradleObjectsFactory, times(1)).createTestEvent(any());
        verify(cradleObjectsFactory, never()).createTestEventBatch(any());

        ArgumentCaptor<StoredTestEventWithContent> capture = ArgumentCaptor.forClass(StoredTestEventWithContent.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());

        StoredTestEventWithContent value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEvent(value, first);
    }

    @Test
    @DisplayName("sub-event without message")
    public void testSubEventDelivery() throws IOException, CradleStorageException {
        Event first = createEvent("root-id","sub-event");
        eventStore.handle(deliveryOf(first));

        verify(cradleObjectsFactory, times(1)).createTestEvent(any());
        verify(cradleObjectsFactory, never()).createTestEventBatch(any());

        ArgumentCaptor<StoredTestEventWithContent> capture = ArgumentCaptor.forClass(StoredTestEventWithContent.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());

        StoredTestEventWithContent value = capture.getValue();
        assertNotNull(value, "Captured stored sub-event");
        assertStoredEvent(value, first);
    }

    @Test
    @DisplayName("multiple sub-events without messages")
    public void testMultipleSubEventsDelivery() throws IOException, CradleStorageException {
        Event first = createEvent("root-id","sub-event-first");
        Event second = createEvent("root-id","sub-event-second");
        eventStore.handle(deliveryOf(first, second));

        verify(cradleObjectsFactory, times(2)).createTestEvent(any());
        verify(cradleObjectsFactory, never()).createTestEventBatch(any());

        ArgumentCaptor<StoredTestEventWithContent> capture = ArgumentCaptor.forClass(StoredTestEventWithContent.class);
        verify(storageMock, times(2)).storeTestEventAsync(capture.capture());

        StoredTestEventWithContent value = capture.getAllValues().get(0);
        assertNotNull(value, "Captured first stored event");
        assertStoredEvent(value, first);

        value = capture.getAllValues().get(1);
        assertNotNull(value, "Captured second stored event");
        assertStoredEvent(value, second);
    }

    @Test
    @DisplayName("Event batch with two events without messages")
    public void testEventsBatchDelivery() throws IOException, CradleStorageException {
        String parentId = "root-id";
        Event first = createEvent(parentId,"sub-event-first");
        Event second = createEvent(parentId,"sub-event-second");
        eventStore.handle(deliveryOf(parentId, first, second));

        verify(cradleObjectsFactory, never()).createTestEvent(any());
        verify(cradleObjectsFactory, times(1)).createTestEventBatch(any());

        ArgumentCaptor<StoredTestEventBatch> capture = ArgumentCaptor.forClass(StoredTestEventBatch.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());

        StoredTestEventBatch value = capture.getValue();
        assertNotNull(value, "Captured stored event batch");
        assertStoredEventBatch(value, parentId, first, second);
    }

    @Test
    @DisplayName("Root event with three messages")
    public void testRootEventWithMessagesDelivery() throws IOException, CradleStorageException {
        Event first = createEvent("root", 3);
        eventStore.handle(deliveryOf(first));

        verify(cradleObjectsFactory, times(1)).createTestEvent(any());
        verify(cradleObjectsFactory, never()).createTestEventBatch(any());

        ArgumentCaptor<StoredTestEventWithContent> captureEvent = ArgumentCaptor.forClass(StoredTestEventWithContent.class);
        verify(storageMock, times(1)).storeTestEventAsync(captureEvent.capture());

        StoredTestEventWithContent capturedEvent = captureEvent.getValue();
        assertNotNull(capturedEvent, "Captured stored event");
        assertStoredEvent(capturedEvent, first);

        assertEventAndStoredEvent(first, capturedEvent.getId(), capturedEvent.getMessageIds());
    }

    @Test
    @DisplayName("Event batch with two events and messages")
    public void testEventsBatchDeliveryWithMessages() throws IOException, CradleStorageException {
        String parentId = "root-id";
        Event first = createEvent(parentId,"sub-event-first", 2);
        Event second = createEvent(parentId,"sub-event-second", 2);
        eventStore.handle(deliveryOf(parentId, first, second));

        verify(cradleObjectsFactory, never()).createTestEvent(any());
        verify(cradleObjectsFactory, times(1)).createTestEventBatch(any());

        ArgumentCaptor<StoredTestEventBatch> capture = ArgumentCaptor.forClass(StoredTestEventBatch.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());

        StoredTestEventBatch storedTestEventBatch = capture.getValue();
        assertNotNull(storedTestEventBatch, "Captured stored event batch");
        assertStoredEventBatch(storedTestEventBatch, parentId, first, second);

        List<BatchedStoredTestEvent> batchedStoredTestEvents = new ArrayList<>(storedTestEventBatch.getTestEvents());
        StoredTestEventId firstStoredEventId = batchedStoredTestEvents.get(0).getId();
        StoredTestEventId secondStoredEventId = batchedStoredTestEvents.get(1).getId();

        Map<StoredTestEventId, Collection<StoredMessageId>> eventIdToMessageIds= storedTestEventBatch.getMessageIdsMap();
        assertEventAndStoredEvent(first, firstStoredEventId, eventIdToMessageIds.get(firstStoredEventId));
        assertEventAndStoredEvent(second, secondStoredEventId, eventIdToMessageIds.get(secondStoredEventId));
    }

    private void assertEventAndStoredEvent(Event event, StoredTestEventId capturedEventId, Collection<StoredMessageId> messageIds) {
        assertNotNull(capturedEventId);
        assertEquals(event.getId().getId(), capturedEventId.toString());
        assertAttachedMessages(event, messageIds);
    }

    private static void assertAttachedMessages(Event event, Collection<StoredMessageId> messageIds) {
        assertEquals(
                event.getAttachedMessageIdsList().stream().map(ProtoUtil::toStoredMessageId).collect(Collectors.toSet()),
                new HashSet<>(messageIds)
        );
    }

    private static void assertStoredEventBatch(StoredTestEventBatch actualBatch, String expectedParentId, Event ... expectedEvents) {
        assertEquals(expectedParentId, actualBatch.getParentId().toString(), "Parent event id of event batch");
        assertEquals(expectedEvents.length, actualBatch.getTestEvents().size(), "Event batch size");
        List<BatchedStoredTestEvent> actualEvents = new ArrayList<>(actualBatch.getTestEvents());
        for (int i = 0; i < expectedEvents.length; i++) {
            assertStoredEvent(actualEvents.get(i), expectedEvents[i]);
        }
    }

    private static void assertStoredEvent(StoredTestEventWithContent actual, Event expected) {
        assertEquals(expected.getId().getId(), actual.getId().toString(), "Event id");
        if (expected.hasParentId()) {
            assertEquals(expected.getParentId().getId(), actual.getParentId().toString(), "Parent event id");
        } else {
            assertNull(actual.getParentId(), "Empty parent event id");
        }
        assertEquals(from(expected.getStartTimestamp()), actual.getStartTimestamp(), "Event start timestamp");
        assertEquals(from(expected.getEndTimestamp()), actual.getEndTimestamp(), "Event end timestamp");
        assertEquals(expected.getName(), actual.getName(), "Event name");
        assertEquals(expected.getType(), actual.getType(), "Event type");
        assertArrayEquals(expected.getBody().toByteArray(), actual.getContent(), "Event context");
        assertEquals(ProtoUtil.isSuccess(expected.getStatus()), actual.isSuccess(), "Event status");
        assertAttachedMessages(expected, actual.getMessageIds());
    }

    private Event createEvent(String parentId, String name, int numberOfMessages) {
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

    private Event createEvent(String name) {
        return createEvent(null, name, 0);
    }

    private Event createEvent(String parentId, String name) {
        return createEvent(parentId, name, 0);
    }

    private Event createEvent(String name, int numberOfMessages) {
        return createEvent(null, name, numberOfMessages);
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
    private EventID createEventID(String parentId) {
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

    private EventBatch deliveryOf(String parentId, Event... events) {
        var eventBatchBuilder = EventBatch.newBuilder()
                .addAllEvents(List.of(events));
        if (parentId != null) {
            eventBatchBuilder.setParentEventId(createEventID(parentId));
        }

        return eventBatchBuilder.build();
    }

    private EventBatch deliveryOf(Event... events) {
        return deliveryOf(null, events);
    }

    private static Instant from(TimestampOrBuilder timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}