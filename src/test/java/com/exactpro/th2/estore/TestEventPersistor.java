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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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

    private final Random random = new Random();
    private final CradleStorage storageMock = mock(CradleStorage.class);

    @SuppressWarnings("unchecked")
    private final Consumer<StoredTestEvent> callback = mock(Consumer.class);
    private EventPersistor persistor;

    private CradleObjectsFactory cradleObjectsFactory;

    @BeforeEach
    void setUp() throws IOException, InterruptedException {
        cradleObjectsFactory = spy(new CradleObjectsFactory(MAX_MESSAGE_BATCH_SIZE, MAX_TEST_EVENT_BATCH_SIZE));
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
    public void testSingleEvent() throws IOException {

        Instant timestamp = Instant.now();
        StoredTestEventId parentId = new StoredTestEventId("test-parent");

        TestEventToStore event = createEvent(parentId, "test-id-1", "test-event", timestamp, 12);
        persistor.persist(event, callback);

        pause(EVENT_PERSIST_TIMEOUT);

        ArgumentCaptor<TestEventToStore> capture = ArgumentCaptor.forClass(TestEventToStore.class);
        verify(persistor, times(1)).processTask(any());
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());
        verify(callback, times(1)).accept(any());

        TestEventToStore value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEvent(event, value);
    }

    @Test
    @DisplayName("event batch persistence")
    public void testEventBatch() throws IOException, CradleStorageException {

        StoredTestEventBatch eventBatch = createEventBatch1();
        persistor.persist(eventBatch, callback);

        pause(EVENT_PERSIST_TIMEOUT * 2);

        ArgumentCaptor<StoredTestEventBatch> capture = ArgumentCaptor.forClass(StoredTestEventBatch.class);
        verify(persistor, times(1)).processTask(any());
        verify(storageMock, times(1)).storeTestEventAsync(capture.capture());
        verify(callback, times(1)).accept(any());

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
        persistor.persist(eventBatch, callback);

        pause(EVENT_PERSIST_TIMEOUT * 2);

        ArgumentCaptor<StoredTestEventBatch> capture = ArgumentCaptor.forClass(StoredTestEventBatch.class);
        verify(persistor, times(2)).processTask(any());
        verify(storageMock, times(2)).storeTestEventAsync(capture.capture());
        verify(callback, times(1)).accept(any());

        StoredTestEventBatch value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEventBatch(eventBatch, value);
    }


    @Test
    @DisplayName("failed event is retried limited times")
    public void testEventResubmittedLimitedTimes() throws IOException, CradleStorageException {

        OngoingStubbing<CompletableFuture<Void>> os = when(storageMock.storeTestEventAsync(any()));
        for (int i = 0; i <= MAX_EVENT_PERSIST_RETRIES; i++)
            os = os.thenReturn(CompletableFuture.failedFuture(new IOException("event persistence failure")));
        os.thenReturn(CompletableFuture.completedFuture(null));

        StoredTestEventBatch eventBatch = createEventBatch1();
        persistor.persist(eventBatch, callback);

        pause(EVENT_PERSIST_TIMEOUT * (MAX_EVENT_PERSIST_RETRIES + 1));

        ArgumentCaptor<StoredTestEventBatch> capture = ArgumentCaptor.forClass(StoredTestEventBatch.class);
        verify(persistor, times(MAX_EVENT_PERSIST_RETRIES + 1)).processTask(any());
        verify(storageMock, times(MAX_EVENT_PERSIST_RETRIES + 1)).storeTestEventAsync(capture.capture());
        verify(callback, times(1)).accept(any());

        StoredTestEventBatch value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredEventBatch(eventBatch, value);
    }


    @Test
    @DisplayName("Event persistence is queued by count")
    public void testEventCountQueueing() throws IOException, CradleStorageException, InterruptedException {

        final long storeExecutionTime = EVENT_PERSIST_TIMEOUT * 3;
        final long totalExecutionTime = EVENT_PERSIST_TIMEOUT * 5;

        final int totalEvents = MAX_EVENT_QUEUE_TASK_SIZE + 3;

        // create executor with thread pool size > event queue size to avoid free thread waiting
        final ExecutorService executor = Executors.newFixedThreadPool(MAX_EVENT_QUEUE_TASK_SIZE * 2);

        StoredTestEventBatch eventBatch = createEventBatch1();

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
        verify(callback, after(totalExecutionTime).times(totalEvents)).accept(any());

        executor.shutdown();
        executor.awaitTermination(0, TimeUnit.MILLISECONDS);
    }


    @Test
    @DisplayName("Event persistence is queued by event sizes")
    public void testEventSizeQueueing() throws IOException, InterruptedException {

        final long storeExecutionTime = EVENT_PERSIST_TIMEOUT * 3;
        final long totalExecutionTime = EVENT_PERSIST_TIMEOUT * 6;

        final int totalEvents = 5;
        final int eventCapacityInQueue = 3;

        // create executor with thread pool size > event queue size to avoid free thread waiting
        final ExecutorService executor = Executors.newFixedThreadPool(totalEvents * 2);

        // create events
        final int eventContentSize = (int) MAX_EVENT_QUEUE_DATA_SIZE / eventCapacityInQueue;
        final byte[] content = new byte[eventContentSize];

        StoredTestEventId parentId = new StoredTestEventId("test-parent");
        TestEventToStore events[] = new TestEventToStore[totalEvents];
        for (int i = 0; i < totalEvents; i++)
            events[i] = createEvent(parentId, "test-id-" + i, "test-event", Instant.now(), 1, content);

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
        verify(callback, after(totalExecutionTime).times(totalEvents)).accept(any());

        executor.shutdown();
        executor.awaitTermination(0, TimeUnit.MILLISECONDS);
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
                                         int numberOfMessages,
                                         byte[] content) {

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
                .content(content)
                .success(true)
                .messageIds(messageIds);

        eventBuilder.endTimestamp(Instant.now());
        return eventBuilder.build();
    }


    private TestEventToStore createEvent(StoredTestEventId parentId,
                                         String id,
                                         String name,
                                         Instant timestamp,
                                         int numberOfMessages) {

        return createEvent(parentId, id, name, timestamp,
                numberOfMessages, ("msg-" + random.nextInt()).getBytes(StandardCharsets.UTF_8));
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