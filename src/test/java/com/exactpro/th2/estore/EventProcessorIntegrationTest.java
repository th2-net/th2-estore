/*
 * Copyright 2026 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.estore;

import com.exactpro.cradle.CradleEntitiesFactory;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.test.annotations.Th2AppFactory;
import com.exactpro.th2.test.annotations.Th2IntegrationTest;
import com.exactpro.th2.test.annotations.Th2TestFactory;
import com.exactpro.th2.test.spec.RabbitMqSpec;
import com.google.protobuf.ByteString;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.event.Event.start;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Runs {@link EventProcessor} and {@link EventPersistor} against real RabbitMQ, only {@link CradleStorage} is mocked.
 * The tests check which deliveries are acknowledged and which are returned to the queue.
 */
@Tag("integration-test")
@Th2IntegrationTest
@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class EventProcessorIntegrationTest {
    private static final String PIN = "estore-pin";
    private static final String BOOK = "test-book";
    private static final String SCOPE = "test-scope";
    private static final int MAX_BATCH_SIZE = 1024 * 1024;
    private static final long WAIT_TIMEOUT = 15_000;

    @SuppressWarnings("unused") // used by Th2IntegrationTest extension
    public final RabbitMqSpec mq = RabbitMqSpec.create()
            .configurePins(pins -> pins.configureSubscribers(subscribers ->
                    subscribers.configurePin(PIN, pin -> pin.attributes("event"))));

    private final CradleStorage storage = mock(CradleStorage.class);
    private final ErrorCollector errorCollector = mock(ErrorCollector.class);
    private final CradleEntitiesFactory entitiesFactory = new CradleEntitiesFactory(MAX_BATCH_SIZE, MAX_BATCH_SIZE, 30_000L);

    private EventPersistor persistor;
    private RecordingPersistor recordingPersistor;
    private EventProcessor processor;

    @BeforeEach
    void setUp() {
        // the test instance is shared between tests
        reset(storage, errorCollector);
        persistor = null;
        processor = null;
    }

    @AfterEach
    void tearDown() {
        if (processor != null) {
            processor.close();
        }
        if (persistor != null) {
            persistor.close();
        }
    }

    @Test
    @DisplayName("#164: event bigger than max queue data size is stored and the delivery is acknowledged")
    public void testOversizedEvent(
            @Th2AppFactory CommonFactory appFactory,
            @Th2TestFactory CommonFactory testFactory
    ) throws Exception {
        when(storage.storeTestEventAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
        startEstore(appFactory, 128, 1024L);

        Event oversized = event("oversized", 10 * 1024);
        Event regular = event("regular", 0);
        testFactory.getEventBatchRouter().send(batchOf(oversized, regular), PIN);

        verify(storage, timeout(WAIT_TIMEOUT).times(2)).storeTestEventAsync(any());

        appFactory.close();
        assertEquals(List.of(), awaitQueueContent(testFactory, 0), "Deliveries in queue");
    }

    @Test
    @DisplayName("interrupted delivery isn't rejected when already submitted event fails later")
    public void testInterruptedDeliveryIsNotRejected(
            @Th2AppFactory CommonFactory appFactory,
            @Th2TestFactory CommonFactory testFactory
    ) throws Exception {
        CompletableFuture<Void> firstStore = new CompletableFuture<>();
        when(storage.storeTestEventAsync(any())).thenReturn(firstStore);
        // queue holds only one task, so the second event blocks the consumer thread
        startEstore(appFactory, 1, MAX_BATCH_SIZE);

        testFactory.getEventBatchRouter().send(batchOf(event("first", 0), event("second", 0), event("third", 0)), PIN);

        Thread consumer = recordingPersistor.awaitBlockedConsumer(2);
        consumer.interrupt();
        assertTrue(recordingPersistor.interrupted.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS), "Consumer thread is interrupted");

        // already submitted event fails without retry (maxRetryCount = 0), that calls Callback.onFail for the delivery
        firstStore.completeExceptionally(new IOException("test"));
        verify(errorCollector, timeout(WAIT_TIMEOUT)).collect("Failed to store an event batch, aborting after several executions");
        verify(storage, times(1)).storeTestEventAsync(any());

        // the delivery is neither acknowledged nor rejected, so broker returns it to the queue on connection close
        appFactory.close();
        assertEquals(List.of(List.of("first", "second", "third")), awaitQueueContent(testFactory, 1), "Deliveries in queue");
    }

    @Test
    @DisplayName("deliveries are returned to the queue when factory is closed with blocked consumer thread")
    public void testShutdownWithBlockedConsumer(
            @Th2AppFactory CommonFactory appFactory,
            @Th2TestFactory CommonFactory testFactory
    ) throws Exception {
        CompletableFuture<Void> firstStore = new CompletableFuture<>();
        when(storage.storeTestEventAsync(any())).thenReturn(firstStore);
        // queue holds only one task, so the second delivery blocks the consumer thread
        startEstore(appFactory, 1, MAX_BATCH_SIZE);

        testFactory.getEventBatchRouter().send(batchOf(event("first", 0)), PIN);
        testFactory.getEventBatchRouter().send(batchOf(event("second", 0)), PIN);
        recordingPersistor.awaitBlockedConsumer(2);

        // the same order as estore shutdown hook uses, the factory interrupts the consumer thread on its executor shutdown
        processor.close();
        appFactory.close();
        assertTrue(recordingPersistor.interrupted.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS), "Consumer thread is interrupted");

        verify(storage, times(1)).storeTestEventAsync(any());
        assertEquals(
                List.of(List.of("first"), List.of("second")),
                awaitQueueContent(testFactory, 2),
                "Deliveries in queue"
        );

        // late store completion can't acknowledge the delivery via closed connection, it is a duplicate after restart
        firstStore.complete(null);
    }

    @Test
    @DisplayName("each delivery is stored or returned to the queue on shutdown in estore order")
    public void testGracefulShutdown(
            @Th2AppFactory CommonFactory appFactory,
            @Th2TestFactory CommonFactory testFactory
    ) throws Exception {
        CompletableFuture<Void> firstStore = new CompletableFuture<>();
        Set<String> stored = ConcurrentHashMap.newKeySet();
        when(storage.storeTestEventAsync(any())).thenAnswer(invocation -> {
            TestEventToStore event = invocation.getArgument(0);
            if ("first".equals(event.getName())) {
                return firstStore.thenRun(() -> stored.add(event.getName()));
            }
            stored.add(event.getName());
            return CompletableFuture.completedFuture(null);
        });
        // queue holds only one task, so the second delivery blocks the consumer thread
        startEstore(appFactory, 1, MAX_BATCH_SIZE);

        testFactory.getEventBatchRouter().send(batchOf(event("first", 0)), PIN);
        testFactory.getEventBatchRouter().send(batchOf(event("second", 0)), PIN);
        testFactory.getEventBatchRouter().send(batchOf(event("third", 0)), PIN);
        recordingPersistor.awaitBlockedConsumer(2);

        // the same order as estore shutdown hook uses
        processor.close();
        processor = null;
        // persistor waits for in-flight store of the first event
        Thread closer = new Thread(persistor::close, "test-persistor-closer");
        closer.start();
        awaitCondition(() -> closer.getState() == Thread.State.WAITING || closer.getState() == Thread.State.TIMED_WAITING,
                () -> "Persistor close isn't waiting, state: " + closer.getState());
        firstStore.complete(null);
        closer.join(WAIT_TIMEOUT);
        assertFalse(closer.isAlive(), "Persistor is closed");
        persistor = null;
        appFactory.close();

        // depending on timing the second event can be stored after persistor stop, but nothing is lost
        assertTrue(stored.contains("first"), "First event is stored, stored: " + stored);
        Set<String> queued = new HashSet<>();
        awaitCondition(() -> {
            queued.clear();
            try {
                readQueue(testFactory).forEach(queued::addAll);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return stored.size() + queued.size() >= 3;
        }, () -> "Lost deliveries, stored: " + stored + ", queued: " + queued);
        assertFalse(queued.contains("first"), "First delivery is acknowledged, queued: " + queued);
        Set<String> all = new HashSet<>(stored);
        all.addAll(queued);
        assertEquals(Set.of("first", "second", "third"), all, "Stored or queued events");
    }

    private void startEstore(CommonFactory appFactory, int maxTaskCount, long maxTaskDataSize) throws InterruptedException {
        Configuration config = new Configuration(maxTaskCount, maxTaskDataSize, 0, 10L, 1, 5_000L);
        persistor = new EventPersistor(errorCollector, config, storage);
        persistor.start();
        recordingPersistor = new RecordingPersistor(persistor);
        processor = new EventProcessor(errorCollector, appFactory.getEventBatchRouter(), entitiesFactory, recordingPersistor);
        processor.start();
    }

    private static Event event(String name, int bodySize) throws Exception {
        Event event = start().name(name).toProto(BOOK, SCOPE);
        if (bodySize == 0) {
            return event;
        }
        return event.toBuilder().setBody(ByteString.copyFrom(new byte[bodySize])).build();
    }

    private static EventBatch batchOf(Event... events) {
        return EventBatch.newBuilder().addAllEvents(List.of(events)).build();
    }

    /**
     * Waits until the queue has expected number of ready messages and returns event names of each delivery.
     */
    private static List<List<String>> awaitQueueContent(CommonFactory testFactory, int expectedCount) throws Exception {
        try (Connection connection = connectionFactory(testFactory.getRabbitMqConfiguration()).newConnection();
             Channel channel = connection.createChannel()) {
            awaitCondition(() -> messageCount(channel) == expectedCount,
                    () -> "Expected " + expectedCount + " messages in queue but was " + messageCount(channel));
            return readQueue(channel);
        }
    }

    private static List<List<String>> readQueue(CommonFactory testFactory) throws Exception {
        try (Connection connection = connectionFactory(testFactory.getRabbitMqConfiguration()).newConnection();
             Channel channel = connection.createChannel()) {
            return readQueue(channel);
        }
    }

    /**
     * Reads event names of each delivery in queue, messages are returned to the queue on channel close.
     * Every delivery must be marked as redelivered.
     */
    private static List<List<String>> readQueue(Channel channel) throws Exception {
        List<List<String>> deliveries = new ArrayList<>();
        GetResponse response;
        while ((response = channel.basicGet(PIN, false)) != null) {
            assertTrue(response.getEnvelope().isRedeliver(), "Delivery is redelivered");
            deliveries.add(EventBatch.parseFrom(response.getBody()).getEventsList().stream()
                    .map(Event::getName)
                    .collect(Collectors.toList()));
        }
        return deliveries;
    }

    private static int messageCount(Channel channel) {
        try {
            return channel.queueDeclarePassive(PIN).getMessageCount();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ConnectionFactory connectionFactory(RabbitMQConfiguration configuration) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(configuration.getHost());
        factory.setPort(configuration.getPort());
        if (!configuration.getVHost().isBlank()) {
            factory.setVirtualHost(configuration.getVHost());
        }
        factory.setUsername(configuration.getUsername());
        factory.setPassword(configuration.getPassword());
        return factory;
    }

    private static void awaitCondition(BooleanSupplier condition, Supplier<String> message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail(message.get());
            }
            Thread.sleep(50);
        }
    }

    /**
     * Records the RabbitMQ consumer thread which calls {@link EventPersistor#persist} and its interruption.
     */
    private static class RecordingPersistor implements Persistor<TestEventToStore> {
        private final EventPersistor delegate;
        private final AtomicReference<Thread> consumer = new AtomicReference<>();
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch interrupted = new CountDownLatch(1);

        private RecordingPersistor(EventPersistor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void persist(TestEventToStore data, Callback<TestEventToStore> callback) throws Exception {
            consumer.set(Thread.currentThread());
            calls.incrementAndGet();
            try {
                delegate.persist(data, callback);
            } catch (InterruptedException e) {
                interrupted.countDown();
                throw e;
            }
        }

        /**
         * Waits until {@link #persist} is called expected number of times and the last call is blocked
         */
        private Thread awaitBlockedConsumer(int expectedCalls) throws InterruptedException {
            awaitCondition(() -> {
                Thread thread = consumer.get();
                return calls.get() == expectedCalls && thread != null && thread.getState() == Thread.State.WAITING;
            }, () -> "Consumer thread isn't blocked, persist calls: " + calls.get() + ", thread: " + consumer.get());
            return consumer.get();
        }
    }
}
