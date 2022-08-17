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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestEventPersistor {

    private static final int  MAX_MESSAGE_BATCH_SIZE      = 16 * 1024;
    private static final int  MAX_TEST_EVENT_BATCH_SIZE   = 16 * 1024;

    private static final long EVENT_PERSIST_TIMEOUT       = 100;

    private static final int  MAX_EVENT_PERSIST_RETRIES   = 2;
    private static final int  MAX_EVENT_QUEUE_TASK_SIZE   = 8;
    private static final long MAX_EVENT_QUEUE_DATA_SIZE   = 10_000L;

    private static final String BOOK_NAME = "test-book";
    private static final String SCOPE = "test-scope";

    private final Random random = new Random();
    private final CradleStorage storageMock = mock(CradleStorage.class);
    private EventPersistor persistor;

    private CradleEntitiesFactory cradleEntitiesFactory;

    @BeforeEach
    void setUp() throws CradleStorageException, IOException, InterruptedException {
        cradleEntitiesFactory = spy(new CradleEntitiesFactory(MAX_MESSAGE_BATCH_SIZE, MAX_TEST_EVENT_BATCH_SIZE));
        doReturn(CompletableFuture.completedFuture(null)).when(storageMock).storeTestEventAsync(any());

        Configuration config = new Configuration(MAX_EVENT_QUEUE_TASK_SIZE, MAX_EVENT_PERSIST_RETRIES, 10L, MAX_EVENT_QUEUE_DATA_SIZE);
        persistor = spy(new EventPersistor(config, storageMock));
        persistor.start();
    }

    @AfterEach
    void dispose() {
        persistor.close();
        reset(storageMock);
    }

    @Test
    @DisplayName("single event persistence")
    public void testSingleEvent() throws IOException, CradleStorageException {

        BookId bookId = new BookId(BOOK_NAME);
        Instant timestamp = Instant.now();
        StoredTestEventId parentId = new StoredTestEventId(bookId, SCOPE, timestamp, "test-parent");

        TestEventSingleToStore event = createEvent(bookId, SCOPE, parentId, "test-id-1", "test-event", timestamp, 12);

        persistor.persist(event);

        pause(EVENT_PERSIST_TIMEOUT);

        ArgumentCaptor<TestEventToStore> capture = ArgumentCaptor.forClass(TestEventToStore.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());
        verify(persistor, times(1)).processTask(any());

        TestEventToStore value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEvent(event, value);
    }

    @Test
    @DisplayName("event batch persistence")
    public void testEventBatch() throws IOException, CradleStorageException {

        TestEventBatchToStore eventBatch = createEventBatch1();
        persistor.persist(eventBatch);

        pause(EVENT_PERSIST_TIMEOUT * 2);

        ArgumentCaptor<TestEventToStore> capture = ArgumentCaptor.forClass(TestEventToStore.class);
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());
        verify(persistor, times(1)).processTask(any());

        TestEventToStore value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEvent(eventBatch, value);
    }


    @Test
    @DisplayName("failed event is retried")
    public void testEventResubmitted() throws IOException, CradleStorageException {

        when(storageMock.storeTestEventAsync(any()))
                .thenReturn(CompletableFuture.failedFuture(new IOException("event persistence failure")))
                .thenReturn(CompletableFuture.completedFuture(null));

        TestEventBatchToStore eventBatch = createEventBatch1();
        persistor.persist(eventBatch);

        pause(EVENT_PERSIST_TIMEOUT * 2);

        ArgumentCaptor<TestEventToStore> capture = ArgumentCaptor.forClass(TestEventToStore.class);
        verify(persistor, times(2)).processTask(any());
        verify(storageMock, times(2)).storeTestEventAsync(capture.capture());

        TestEventToStore value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEvent(eventBatch, value);
    }


    private TestEventBatchToStore createEventBatch1() throws CradleStorageException{

        BookId bookId = new BookId(BOOK_NAME);
        Instant timestamp = Instant.now();
        StoredTestEventId parentId = new StoredTestEventId(bookId, SCOPE, timestamp, "test-parent");

        TestEventSingleToStore first = createEvent(bookId, SCOPE, parentId, "test-id-1", "test-event", timestamp, 12);
        TestEventSingleToStore second = createEvent(bookId, SCOPE, parentId, "test-id-2", "test-event", timestamp, 14);

        return deliveryOf(bookId, SCOPE, parentId, "test-batch", timestamp, first, second);
    }

    private void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }

    private static Map<StoredTestEventId, TestEventSingle> mapOf(Collection<BatchedStoredTestEvent> collection) {
        if (collection != null)
            return collection.stream().collect(Collectors.toMap(BatchedStoredTestEvent::getId, v -> v));
        else
            return Collections.emptyMap();
    }

    private static void assertStoredEvent(TestEventToStore expected, TestEventToStore actual) {

        assertEquals(expected.getBookId(), actual.getBookId(), "Event book id");
        assertEquals(expected.getScope(), actual.getScope(), "Event scope");
        assertEquals(expected.getId(), actual.getId(), "Event id");

        if (expected.getParentId() != null)
            assertEquals(expected.getParentId(), actual.getParentId(), "Parent event id");
        else
            assertNull(actual.getParentId(), "Empty parent event id");

        assertEquals(expected.isBatch(), actual.isBatch(), "Event is batch");
        assertEquals(expected.isSingle(), actual.isSingle(), "Event is single");
        assertEquals(expected.getType(), actual.getType(), "Event type");
        assertEquals(expected.isSuccess(), actual.isSuccess(), "Event status");

        assertEquals(expected.getEndTimestamp(), actual.getEndTimestamp(), "Event end timestamp");

        if (expected.getMessages() != null)
            assertEquals(expected.getMessages(), actual.getMessages(), "Event message id");
        else
            assertNull(actual.getMessages(), "Event message id");

        if (expected.isSingle()) {
            assertArrayEquals(expected.asSingle().getContent(), actual.asSingle().getContent(), "Event content");
        }

        if (expected.isBatch()) {
            assertEquals(expected.asBatch().getTestEventsCount(), actual.asBatch().getTestEventsCount());
            Map<StoredTestEventId, TestEventSingle> expectedEvents = mapOf(expected.asBatch().getTestEvents());
            Map<StoredTestEventId, TestEventSingle> actualEvents = mapOf(actual.asBatch().getTestEvents());

            for (TestEventSingle expectedSingle : expectedEvents.values()) {
                StoredTestEventId id = expectedSingle.getId();
                TestEventSingle actualSingle = actualEvents.get(id);
                assertNotNull(actualSingle, "Event inside batch " + id);
                assertArrayEquals(expectedSingle.getContent(), actualSingle.getContent(), "Event content for batched event " + id);
            }
        }
    }

    private TestEventSingleToStore createEvent(BookId bookId,
                                               String scope,
                                               StoredTestEventId parentId,
                                               String id,
                                               String name,
                                               Instant timestamp,
                                               int numberOfMessages) throws CradleStorageException {

        TestEventSingleToStoreBuilder eventBuilder = new TestEventSingleToStoreBuilder()
                .id(bookId, scope, timestamp, id)
                .name(name)
                .parentId(parentId)
                .type("test-event_type")
                .content(("msg-" + random.nextInt()).getBytes(StandardCharsets.UTF_8))
                .success(true);

        for (int i = 0; i < numberOfMessages; i++) {
            eventBuilder.message(
                    new StoredMessageId(
                        bookId,
                        "session-alias-" + random.nextInt(),
                        Direction.SECOND,
                        timestamp,
                        random.nextLong())
            );
        }

        eventBuilder.endTimestamp(Instant.now());
        return eventBuilder.build();
    }


    private TestEventBatchToStore deliveryOf(BookId bookId,
                                             String scope,
                                             StoredTestEventId parentId,
                                             String name,
                                             Instant timestamp,
                                             TestEventSingleToStore... events)
    throws CradleStorageException {

        StoredTestEventId batchId = new StoredTestEventId(bookId, scope, timestamp, "test_event_batch");

        TestEventBatchToStore batch = cradleEntitiesFactory.testEventBatchBuilder()
                .id(batchId)
                .parentId(parentId)
                .name(name)
                .build();

        for (TestEventSingleToStore event: events)
            batch.addTestEvent(event);

        return batch;
    }

}