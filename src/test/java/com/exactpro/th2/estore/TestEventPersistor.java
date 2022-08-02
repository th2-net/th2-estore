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

import com.exactpro.cradle.CradleObjectsFactory;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.*;
import com.exactpro.cradle.utils.CradleStorageException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestEventPersistor {

    private static final int MAX_MESSAGE_BATCH_SIZE = 1024*1024;
    private static final int MAX_TEST_EVENT_BATCH_SIZE = 1024*1024;

    private static final long EVENT_PERSIST_TIMEOUT = EventPersistor.POLL_WAIT_TIMEOUT_MILLIS * 2;

    private final Random random = new Random();
    private final CradleStorage storageMock = mock(CradleStorage.class);
    private EventPersistor persistor;

    private CradleObjectsFactory cradleObjectsFactory;

    @BeforeEach
    void setUp() throws IOException {
        cradleObjectsFactory = spy(new CradleObjectsFactory(MAX_MESSAGE_BATCH_SIZE, MAX_TEST_EVENT_BATCH_SIZE));
        doReturn(CompletableFuture.completedFuture(null)).when(storageMock).storeTestEventAsync(any());

        persistor = spy(new EventPersistor(storageMock));
        persistor.start();
    }

    @AfterEach
    void dispose() {
        persistor.close();
    }

    @Test
    @DisplayName("single event persistence")
    public void testSingleEvent() throws IOException {

        Instant timestamp = Instant.now();
        StoredTestEventId parentId = new StoredTestEventId("test-parent");

        TestEventToStore event = createEvent(parentId, "test-id-1", "test-event", timestamp, 12);

        persistor.persist(event);

        pause(EVENT_PERSIST_TIMEOUT);

        ArgumentCaptor<TestEventToStore> capture = ArgumentCaptor.forClass(TestEventToStore.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());
        verify(persistor, times(1)).storeEvent(any());

        TestEventToStore value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEvent(event, value);
    }

    @Test
    @DisplayName("event batch persistence")
    public void testEventBatch() throws IOException, CradleStorageException {

        StoredTestEventBatch eventBatch = createEventBatch1();
        persistor.persist(eventBatch);

        pause(EVENT_PERSIST_TIMEOUT * 2);

        ArgumentCaptor<StoredTestEventBatch> capture = ArgumentCaptor.forClass(StoredTestEventBatch.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());
        verify(persistor, times(1)).storeEvent(any());

        StoredTestEventBatch value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEventBatch(eventBatch, value);
    }


    @Test
    @DisplayName("failed event is retried")
    public void testEventResubmitted() throws IOException, CradleStorageException {

        when(storageMock.storeTestEventAsync(any()))
                .thenReturn(CompletableFuture.failedFuture(new IOException("event persistence failure")))
                .thenReturn(CompletableFuture.completedFuture(null));

        StoredTestEventBatch eventBatch = createEventBatch1();
        persistor.persist(eventBatch);

        pause(EVENT_PERSIST_TIMEOUT * 2);

        ArgumentCaptor<StoredTestEventBatch> capture = ArgumentCaptor.forClass(StoredTestEventBatch.class);
        verify(storageMock, times(2)).storeTestEventAsync(capture.capture());
        verify(persistor, times(2)).storeEvent(any());

        StoredTestEventBatch value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEventBatch(eventBatch, value);
    }


    private StoredTestEventBatch createEventBatch1() throws CradleStorageException{

        Instant timestamp = Instant.now();
        StoredTestEventId parentId = new StoredTestEventId("test-parent");

        TestEventToStore first = createEvent(parentId, "test-id-1", "test-event", timestamp, 12);
        TestEventToStore second = createEvent(parentId, "test-id-2", "test-event", timestamp, 14);

        return deliveryOf(parentId, "test-batch", first, second);
    }

    private void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }


    private static void assertStoredEventBatch(StoredTestEventBatch expected, StoredTestEventBatch actual) {

        assertEquals(expected.getId(), actual.getId(), "Event batch id");
        assertEquals(expected.getStartTimestamp(), actual.getStartTimestamp(), "Event batch start time");
        assertEquals(expected.getEndTimestamp(), actual.getEndTimestamp(), "Event batch end time");
        assertEquals(expected.getParentId(), actual.getParentId(), "Parent event id of event batch");
        assertEquals(expected.getTestEventsCount(), actual.getTestEventsCount(), "Event count inside batch");

        List<BatchedStoredTestEvent> expectedEvents = new ArrayList<>(expected.getTestEvents());
        List<BatchedStoredTestEvent> actualEvents = new ArrayList<>(actual.getTestEvents());
        for (int i = 0; i < expectedEvents.size(); i++) {
            assertStoredEvent(actualEvents.get(i), expectedEvents.get(i));
        }
    }

    private static void assertStoredEvent(StoredTestEventWithContent expected, StoredTestEventWithContent actual) {

        assertEquals(expected.getId(), actual.getId(), "Event id");

        if (expected.getParentId() != null)
            assertEquals(expected.getParentId(), actual.getParentId(), "Parent event id");
        else
            assertNull(actual.getParentId(), "Empty parent event id");

        assertEquals(expected.getName(), actual.getName(), "Event name");
        assertEquals(expected.getType(), actual.getType(), "Event type");
        assertEquals(expected.isSuccess(), actual.isSuccess(), "Event status");

        assertEquals(expected.getStartTimestamp(), actual.getStartTimestamp(), "Event start timestamp");
        assertEquals(expected.getEndTimestamp(), actual.getEndTimestamp(), "Event end timestamp");

        assertEquals(expected.getMessageIds(), actual.getMessageIds(), "Event message ids");
        assertArrayEquals(expected.getContent(), actual.getContent(), "Event body");
    }


    private TestEventToStore createEvent(StoredTestEventId parentId,
                                         String id,
                                         String name,
                                         Instant timestamp,
                                         int numberOfMessages) {

        Collection<StoredMessageId> messageIds = new ArrayList<>(numberOfMessages);
        for (int i = 0; i < numberOfMessages; i++)
            messageIds.add(
                    new StoredMessageId(
                            "session-alias-" + random.nextInt(),
                            Direction.SECOND,
                            random.nextLong())
            );

        TestEventToStoreBuilder eventBuilder = new TestEventToStoreBuilder()
                .id(new StoredTestEventId(id))
                .startTimestamp(timestamp)
                .name(name)
                .parentId(parentId)
                .type("test-event_type")
                .content(("msg-" + random.nextInt()).getBytes(StandardCharsets.UTF_8))
                .success(true)
                .messageIds(messageIds);

        eventBuilder.endTimestamp(Instant.now());
        return eventBuilder.build();
    }


    private StoredTestEventBatch deliveryOf(StoredTestEventId parentId,
                                             String name,
                                             TestEventToStore... events)
            throws CradleStorageException {

        StoredTestEventId batchId = new StoredTestEventId("test_event_batch");

        TestEventBatchToStore batch = new TestEventBatchToStoreBuilder()
                .id(batchId)
                .parentId(parentId)
                .name(name)
                .build();

        StoredTestEventBatch cc = cradleObjectsFactory.createTestEventBatch(batch);

        for (TestEventToStore event: events)
            cc.addTestEvent(event);

        return new StoredTestEventBatch(batch);
    }
}