/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.testevents.BatchedStoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventSingle;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.taskutils.StartableRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.utils.ExecutorServiceUtilsKt.shutdownGracefully;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("SameParameterValue")
public class TestEventPersistor {

    private static final int  MAX_MESSAGE_BATCH_SIZE      = 16 * 1024;
    private static final int  MAX_TEST_EVENT_BATCH_SIZE   = 16 * 1024;

    private static final long EVENT_PERSIST_TIMEOUT       = 100;

    private static final int  MAX_EVENT_PERSIST_RETRIES   = 2;
    private static final int  MAX_EVENT_QUEUE_TASK_SIZE   = 8;
    private static final long MAX_EVENT_QUEUE_DATA_SIZE   = 10_000L;
    private static final long STORE_ACTION_REJECTION_THRESHOLD = 30_000L;
    private static final int TASK_PROCESSING_THREADS = 4;
    private static final long WAIT_TIMEOUT = 5000;

    private static final String BOOK_NAME = "test-book";
    private static final String SCOPE = "test-scope";

    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final CradleStorage storageMock = mock(CradleStorage.class);
    private final ErrorCollector errorCollector = mock(ErrorCollector.class);

    @SuppressWarnings("unchecked")
    private final Callback<TestEventToStore> callback = mock(Callback.class);
    private EventPersistor persistor;

    private CradleEntitiesFactory cradleEntitiesFactory;

    @BeforeEach
    void setUp() throws CradleStorageException, IOException, InterruptedException {
        cradleEntitiesFactory = spy(new CradleEntitiesFactory(MAX_MESSAGE_BATCH_SIZE, MAX_TEST_EVENT_BATCH_SIZE, STORE_ACTION_REJECTION_THRESHOLD));
        doReturn(CompletableFuture.completedFuture(null)).when(storageMock).storeTestEventAsync(any());

        Configuration config = new Configuration(MAX_EVENT_QUEUE_TASK_SIZE, MAX_EVENT_QUEUE_DATA_SIZE, MAX_EVENT_PERSIST_RETRIES, 10L, TASK_PROCESSING_THREADS, WAIT_TIMEOUT);
        persistor = spy(new EventPersistor(errorCollector, config, storageMock));
        persistor.start();
    }

    @AfterEach
    void dispose() {
        persistor.close();
        reset(storageMock);
    }

    @Test
    @DisplayName("single event persistence")
    public void testSingleEvent() throws IOException, CradleStorageException, InterruptedException {

        BookId bookId = new BookId(BOOK_NAME);
        Instant timestamp = Instant.now();
        StoredTestEventId parentId = new StoredTestEventId(bookId, SCOPE, timestamp, "test-parent");

        TestEventSingleToStore event = createEvent(bookId, SCOPE, parentId, "test-id-1", "test-event", timestamp, 12);
        persistor.persist(event, callback);

        Thread.sleep(EVENT_PERSIST_TIMEOUT);

        ArgumentCaptor<TestEventToStore> capture = ArgumentCaptor.forClass(TestEventToStore.class);
        verify(persistor, times(1)).processTask(any());
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());
        verify(callback, times(1)).onSuccess(any());
        verify(callback, times(0)).onFail(any());

        TestEventToStore value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEvent(event, value);
    }

    @Test
    @DisplayName("event batch persistence")
    public void testEventBatch() throws IOException, CradleStorageException, InterruptedException {

        TestEventBatchToStore eventBatch = createEventBatch1();
        persistor.persist(eventBatch, callback);

        Thread.sleep(EVENT_PERSIST_TIMEOUT * 2);

        ArgumentCaptor<TestEventToStore> capture = ArgumentCaptor.forClass(TestEventToStore.class);
        verify(persistor, times(1)).processTask(any());
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());
        verify(callback, times(1)).onSuccess(any());
        verify(callback, times(0)).onFail(any());

        TestEventToStore value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEvent(eventBatch, value);
    }


    @Test
    @DisplayName("failed event is retried")
    public void testEventResubmitted() throws IOException, CradleStorageException, InterruptedException {

        when(storageMock.storeTestEventAsync(any()))
                .thenReturn(CompletableFuture.failedFuture(new IOException("event persistence failure")))
                .thenReturn(CompletableFuture.completedFuture(null));

        TestEventBatchToStore eventBatch = createEventBatch1();
        persistor.persist(eventBatch, callback);

        Thread.sleep(EVENT_PERSIST_TIMEOUT * 2);

        ArgumentCaptor<TestEventToStore> capture = ArgumentCaptor.forClass(TestEventToStore.class);
        verify(persistor, times(2)).processTask(any());
        verify(storageMock, times(2)).storeTestEventAsync(capture.capture());
        verify(callback, times(1)).onSuccess(any());
        verify(callback, times(0)).onFail(any());

        TestEventToStore value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEvent(eventBatch, value);
    }

    @Test
    @DisplayName("failed event is retried limited times")
    public void testEventResubmittedLimitedTimes() throws IOException, CradleStorageException, InterruptedException {

        OngoingStubbing<CompletableFuture<Void>> os = when(storageMock.storeTestEventAsync(any()));
        for (int i = 0; i <= MAX_EVENT_PERSIST_RETRIES; i++)
            os = os.thenReturn(CompletableFuture.failedFuture(new IOException("event persistence failure")));
        os.thenReturn(CompletableFuture.completedFuture(null));

        TestEventToStore eventBatch = createEventBatch1();
        persistor.persist(eventBatch, callback);

        Thread.sleep(EVENT_PERSIST_TIMEOUT * (MAX_EVENT_PERSIST_RETRIES + 1));

        ArgumentCaptor<TestEventToStore> capture = ArgumentCaptor.forClass(TestEventToStore.class);
        verify(persistor, times(MAX_EVENT_PERSIST_RETRIES + 1)).processTask(any());
        verify(storageMock, times(MAX_EVENT_PERSIST_RETRIES + 1)).storeTestEventAsync(capture.capture());
        verify(callback, times(0)).onSuccess(any());
        verify(callback, times(1)).onFail(any());

        TestEventToStore value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEvent(eventBatch, value);
    }


    @Test
    @DisplayName("Event persistence is queued by count")
    public void testEventCountQueueing() throws IOException, CradleStorageException {

        final long storeExecutionTime = EVENT_PERSIST_TIMEOUT * 3;
        final long totalExecutionTime = EVENT_PERSIST_TIMEOUT * 5;

        final int totalEvents = MAX_EVENT_QUEUE_TASK_SIZE + 3;

        // create executor with thread pool size > event queue size to avoid free thread waiting
        final ExecutorService executor = Executors.newFixedThreadPool(MAX_EVENT_QUEUE_TASK_SIZE * 2);

        TestEventToStore eventBatch = createEventBatch1();

        when(storageMock.storeTestEventAsync(any()))
                .thenAnswer((ignored) ->  CompletableFuture.runAsync(() -> pause(storeExecutionTime), executor));

        // setup producer thread
        StartableRunnable runnable = StartableRunnable.of(() -> {
            for (int i = 0; i < totalEvents; i++)
                persistor.persist(eventBatch, callback);
        });

        new Thread(runnable).start();
        runnable.awaitReadiness();
        runnable.start();

        verify(storageMock, after(EVENT_PERSIST_TIMEOUT).times(MAX_EVENT_QUEUE_TASK_SIZE)).storeTestEventAsync(any());
        verify(storageMock, after(totalExecutionTime).times(totalEvents)).storeTestEventAsync(any());
        verify(callback, after(totalExecutionTime).times(totalEvents)).onSuccess(any());
        verify(callback, after(totalExecutionTime).times(0)).onFail(any());

        shutdownGracefully(executor, 1, TimeUnit.SECONDS);
    }


    @Test
    @DisplayName("Event persistence is queued by event sizes")
    public void testEventSizeQueueing() throws IOException, CradleStorageException {

        final long storeExecutionTime = EVENT_PERSIST_TIMEOUT * 3;
        final long totalExecutionTime = EVENT_PERSIST_TIMEOUT * 6;

        final int totalEvents = 5;
        final int eventCapacityInQueue = 3;

        // create executor with thread pool size > event queue size to avoid free thread waiting
        final ExecutorService executor = Executors.newFixedThreadPool(totalEvents * 2);

        // create events
        final int eventContentSize = (int) MAX_EVENT_QUEUE_DATA_SIZE / eventCapacityInQueue;
        final byte[] content = new byte[eventContentSize];

        BookId bookId = new BookId(BOOK_NAME);
        Instant timestamp = Instant.now();
        StoredTestEventId parentId = new StoredTestEventId(bookId, SCOPE, timestamp, "test-parent");

        TestEventToStore[] events = new TestEventToStore[totalEvents];
        for (int i = 0; i < totalEvents; i++)
            events[i] = createEvent(bookId, SCOPE, parentId, "test-id-1", "test-event", timestamp, 12, content);

        when(storageMock.storeTestEventAsync(any()))
                .thenAnswer((ignored) ->  CompletableFuture.runAsync(() -> pause(storeExecutionTime), executor));

        // setup producer thread
        StartableRunnable runnable = StartableRunnable.of(() -> {
            for (int i = 0; i < totalEvents; i++)
                persistor.persist(events[i], callback);
        });

        new Thread(runnable).start();
        runnable.awaitReadiness();
        runnable.start();

        verify(storageMock, after(EVENT_PERSIST_TIMEOUT).times(eventCapacityInQueue)).storeTestEventAsync(any());
        verify(storageMock, after(totalExecutionTime).times(totalEvents)).storeTestEventAsync(any());
        verify(callback, after(totalExecutionTime).times(totalEvents)).onSuccess(any());
        verify(callback, after(totalExecutionTime).times(0)).onFail(any());

        shutdownGracefully(executor, 1, TimeUnit.SECONDS);
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
            throw new RuntimeException(e);
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
                                               int numberOfMessages,
                                               byte[] content) throws CradleStorageException {

        TestEventSingleToStoreBuilder eventBuilder = cradleEntitiesFactory.testEventBuilder()
                .id(bookId, scope, timestamp, id)
                .name(name)
                .parentId(parentId)
                .type("test-event_type")
                .content(content)
                .success(true);

        for (int i = 0; i < numberOfMessages; i++) {
            eventBuilder.message(
                    new StoredMessageId(
                        bookId,
                        "session-alias-" + random.nextInt(),
                        Direction.SECOND,
                        timestamp,
                        random.nextLong(Long.MAX_VALUE))
            );
        }

        eventBuilder.endTimestamp(Instant.now());
        return eventBuilder.build();
    }


    private TestEventSingleToStore createEvent(BookId bookId,
                                               String scope,
                                               StoredTestEventId parentId,
                                               String id,
                                               String name,
                                               Instant timestamp,
                                               int numberOfMessages) throws CradleStorageException {
        return createEvent(bookId,
                            scope,
                            parentId,
                            id,
                            name,
                            timestamp,
                            numberOfMessages,
                            ("msg-" + random.nextInt()).getBytes(StandardCharsets.UTF_8));
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