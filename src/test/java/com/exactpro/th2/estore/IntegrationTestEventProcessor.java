/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.BookToAdd;
import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.bean.Message;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback;
import com.exactpro.th2.common.utils.ExecutorServiceUtilsKt;
import com.exactpro.th2.common.utils.message.MessageUtilsKt;
import com.exactpro.th2.test.annotations.Th2AppFactory;
import com.exactpro.th2.test.annotations.Th2IntegrationTest;
import com.exactpro.th2.test.extension.CleanupExtension;
import com.exactpro.th2.test.spec.CradleSpec;
import com.exactpro.th2.test.spec.RabbitMqSpec;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("NewClassNamingConvention")
@Tag("integration")
@Th2IntegrationTest
public class IntegrationTestEventProcessor {
    private static final ManualAckDeliveryCallback.Confirmation DUMMY_CONFIRMATION = new ManualAckDeliveryCallback.Confirmation() {
        @Override
        public void reject() {}
        @Override
        public void confirm() {}
    };
    private static final double ITERATIONS = 2_000D;
    private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTestEventProcessor.class);
    private static final AtomicInteger EVENT_COUNTER = new AtomicInteger();
    private static final String TEST_BOOK = "test_book";
    private static final String TEST_SCOPE = "test-scope";
    public static final String TEST_SESSION_GROUP = "test-session-group";
    public static final String TEST_SESSION_ALIAS_PREFIX = "test-session-alias";

    private EventProcessor processor;

    @SuppressWarnings("unused")
    public final CradleSpec cradleSpec = CradleSpec.Companion.create()
            .disableAutoPages()
            .reuseKeyspace();

    @SuppressWarnings("unused")
    public final RabbitMqSpec rabbitMqSpec = RabbitMqSpec.create().configurePins(
            pinsSpec -> pinsSpec.configureSubscribers(
                    subscribers -> subscribers.configurePin("event",
                            pinSpec -> pinSpec.attributes("event")))
    );

    @BeforeAll
    public static void initStorage(CradleManager manager) throws CradleStorageException, IOException {
        // init database schema
        CradleStorage storage = manager.getStorage();

        createBook(storage, "test_book");
    }

    @BeforeEach
    public void init(@Th2AppFactory CommonFactory appFactory,
                     CleanupExtension.Registry resources) throws InterruptedException {
        Configuration config = new Configuration();
        StoredTestEventId rootEventId = new StoredTestEventId(new BookId(TEST_BOOK), TEST_SCOPE, Instant.now(), "root-id");
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        resources.add("executor", () -> ExecutorServiceUtilsKt.shutdownGracefully(executor, 3, TimeUnit.SECONDS));

        ErrorCollector errorCollector = new ErrorCollector(executor, appFactory.getCradleManager().getStorage().getEntitiesFactory());
        resources.add("error-collector", errorCollector);

        EventPersistor persistor = new EventPersistor(errorCollector, config, appFactory.getCradleManager().getStorage());
        resources.add("event-persistor", persistor);
        persistor.start();

        errorCollector.init(persistor, rootEventId);

        this.processor = new EventProcessor(
                errorCollector,
                appFactory.getEventBatchRouter(),
                appFactory.getCradleManager().getStorage().getEntitiesFactory(),
                persistor
        );
        resources.add("event-processor", persistor);
        this.processor.start();
    }

//    @Timeout(30)
    @ParameterizedTest
    @MethodSource("provideEventsArgs")
    public void testRootEvents(Event.Status status,
                               int batchSize,
                               int contentSize,
                               int sessionAliases,
                               CradleManager manager) throws InterruptedException, CradleStorageException, IOException {
        LOGGER.info("testRootEvents - status: {}, batch size: {}, content size: {}, attached message ids: {}",
                status,
                batchSize,
                contentSize,
                sessionAliases);
        BlockingQueue<TestConfirmation> queue = new LinkedBlockingQueue<>();
        String bookName = "testRootEvents_" + status + '_' + batchSize + '_' + contentSize + '_' + sessionAliases;
        createBook(manager.getStorage(), bookName);

        for (TestStage stage : TestStage.values()) {
            generateRootEvents(bookName, status, batchSize, contentSize, sessionAliases)
                    .limit((long) ITERATIONS)
                    .forEach((packet) -> {
                        TestConfirmation confirmation = new TestConfirmation(packet.statistic);
                        queue.add(confirmation);
                        processor.process(packet.batch, confirmation);
                    });

            List<Statistic> statistics = new ArrayList<>();
            for (int i = 0; i < ITERATIONS; i++) {
                statistics.add(requireNonNull(queue.poll(1, TimeUnit.DAYS),
                        "Element " + i + " isn't present")
                        .awaitComplete()
                );
            }

            LOGGER.info("{} (min/median/max) - preparation: {}, packToProto: {}, processing: {}, rate: {}",
                    stage,
                    calculate(statistics, batchSize, Statistic::getPreparation),
                    calculate(statistics, batchSize, Statistic::getPackToProto),
                    calculate(statistics, batchSize, Statistic::getProcessing),
                    calculate(statistics, 1D / batchSize, Statistic::getRate)
            );
        }
    }

    @ParameterizedTest
    @MethodSource("provideEventsArgs")
    public void testEvents(Event.Status status,
                               int batchSize,
                               int contentSize,
                               int sessionAliases,
                               CradleManager manager) throws InterruptedException, CradleStorageException, IOException {
        LOGGER.info("testEvents - status: {}, batch size: {}, content size: {}, attached message ids: {}",
                status,
                batchSize,
                contentSize,
                sessionAliases);
        BlockingQueue<TestConfirmation> queue = new LinkedBlockingQueue<>();
        String bookName = "testEvents_" + status + '_' + batchSize + '_' + contentSize + '_' + sessionAliases;
        createBook(manager.getStorage(), bookName);
        com.exactpro.th2.common.grpc.Event rootEvent = fillEvent(Event.start(), Event.Status.PASSED, "root", bookName, 0, 0)
                .toProto(bookName, TEST_SCOPE);
        processor.process(EventBatch.newBuilder().addEvents(rootEvent).build(), DUMMY_CONFIRMATION);

        EventID rootEventId = rootEvent.getId();

        for (TestStage stage : TestStage.values()) {
            LOGGER.info("Sending - stage: {}", stage);
            long start = System.nanoTime();
            List<Packet> packets = generateEventBatch(rootEventId, status, batchSize, contentSize, sessionAliases)
                    .limit((long) ITERATIONS)
                    .collect(Collectors.toList());

            long prepared = System.nanoTime();
            LOGGER.info("Prepared - stage: {}, batch rate: {}, event rate: {}",
                    stage,
                    ITERATIONS / (prepared - start) * 1_000_000_000,
                    ITERATIONS / (prepared - start) * 1_000_000_000 * batchSize);
            packets.forEach((packet) -> {
                        TestConfirmation confirmation = new TestConfirmation(packet.statistic);
                        queue.add(confirmation);
                        processor.process(packet.batch, confirmation);
                    });

            long sent = System.nanoTime();
            LOGGER.info("Sent - stage: {}, batch rate: {}, event rate: {}",
                    stage,
                    ITERATIONS / (sent - prepared) * 1_000_000_000,
                    ITERATIONS / (sent - prepared) * 1_000_000_000 * batchSize);
            List<Statistic> statistics = new ArrayList<>();
            for (int i = 0; i < ITERATIONS; i++) {
                statistics.add(requireNonNull(queue.poll(1, TimeUnit.DAYS),
                        "Element " + i + " isn't present")
                        .awaitComplete()
                );
            }

            LOGGER.info("Complete - stage: {}, batch rate: {}, event rate: {}",
                    stage,
                    ITERATIONS / (System.nanoTime() - prepared) * 1_000_000_000,
                    ITERATIONS / (System.nanoTime() - prepared) * 1_000_000_000 * batchSize);
            LOGGER.info("{} (min/median/max) - preparation: {}, packToProto: {}, processing: {}, rate: {}",
                    stage,
                    calculate(statistics, batchSize, Statistic::getPreparation),
                    calculate(statistics, batchSize, Statistic::getPackToProto),
                    calculate(statistics, batchSize, Statistic::getProcessing),
                    calculate(statistics, 1D / batchSize, Statistic::getRate)
            );
        }
    }

    private Stream<Packet> generateRootEvents(String book, Event.Status status,
                                              int batchSize,
                                              int contentSize,
                                              int attachedMessages) {
        return Stream.generate(() -> {
            long start = System.currentTimeMillis();
            List<Event> eventList = IntStream.range(0, batchSize)
                    .mapToObj(bIndex -> fillEvent(Event.start(),
                            status,
                            "root",
                            book,
                            contentSize,
                            attachedMessages)).collect(Collectors.toList());
            long toProto = System.currentTimeMillis();
            EventBatch.Builder batchBuilder = EventBatch.newBuilder();
            eventList.stream()
                    .map(eventBuilder -> {
                        try {
                            return eventBuilder.toProto(book, TEST_SCOPE);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).forEach(batchBuilder::addEvents);
            EventBatch batch = batchBuilder.build();
            return new Packet(batch, start, toProto);
        });
    }

    private Stream<Packet> generateEventBatch(EventID parentEventId,
                                              Event.Status status,
                                              int batchSize,
                                              int contentSize,
                                              int sessionAliases) {
        String book = parentEventId.getBookName();
        return Stream.generate(() -> {
            long start = System.currentTimeMillis();
            Event mainEventBuilder = fillEvent(Event.start(),
                    status,
                    "main",
                    book,
                    contentSize,
                    sessionAliases);

            for (int item = 0; item < batchSize; item++) {
                fillEvent(mainEventBuilder.addSubEventWithSamePeriod(),
                        status,
                        "sub",
                        book,
                        contentSize,
                        sessionAliases);
            }
            long toProto = System.currentTimeMillis();
            try {
                EventBatch batch = mainEventBuilder.toBatchProto(parentEventId);
                return new Packet(batch, start, toProto);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static Event fillEvent(Event eventBuilder, Event.Status status, String name, String book, int contentSize, int sessionAliases) {
        eventBuilder.name(name + '-' + EVENT_COUNTER.incrementAndGet())
                .type(name)
                .status(status);
        if (contentSize > 0) {
            eventBuilder.bodyData(genrateMessage(contentSize));
        }
        for (int i = 0; i < sessionAliases; i++) {
            for (Direction direction : Set.of(Direction.FIRST, Direction.SECOND)) {
                MessageID.Builder msgBuilder = MessageID.newBuilder();
                msgBuilder.setBookName(book)
                        .setDirection(direction)
                        .setTimestamp(MessageUtilsKt.toTimestamp(Instant.now()))
                        .setSequence(i + 1);
                msgBuilder.getConnectionIdBuilder()
                        .setSessionGroup(TEST_SESSION_GROUP)
                        .setSessionAlias(TEST_SESSION_ALIAS_PREFIX + i);
                eventBuilder.messageID(msgBuilder.build());
            }
        }
        return eventBuilder;
    }

    private static <T extends Number> String calculate(List<Statistic> statistics, double divider, Function<Statistic, T> getFunc) {
        List<Double> values = statistics.stream().map(getFunc).map(v -> v.doubleValue() / divider).sorted().collect(Collectors.toList());
        return String.valueOf(values.stream().min(Double::compareTo).orElseThrow()) + '/' +
                values.get(values.size() / 2) + '/' +
                values.stream().max(Double::compareTo).orElseThrow();
    }
    @NotNull
    private static Message genrateMessage(int contentSize) {
        Message bodyData = new Message();
        bodyData.setData(RandomStringUtils.random(contentSize, true, true));
        return bodyData;
    }

    private static Stream<Arguments> provideEventsArgs() {
        return Stream.of(
                // status, batch size, content size, attached messages
//                Arguments.of(Event.Status.PASSED, 1, 500, 100),
//                Arguments.of(Event.Status.PASSED, 10, 500, 100),
                Arguments.of(Event.Status.PASSED, 100, 500, 5),
                Arguments.of(Event.Status.PASSED, 5, 2500, 4)
//                Arguments.of(Event.Status.PASSED, 1000, 500, 100)
        );
    }

    private static void createBook(CradleStorage storage, String bookName) throws CradleStorageException, IOException {
        storage.addBook(new BookToAdd(bookName, Instant.now()));
        storage.addPage(new BookId(bookName), "test-page", Instant.now(), "test-comment");
    }

    private enum TestStage {
        WARM_UP,
        TEST
    }
    private static class Statistic {
        private final long start;
        private final long toProto;
        private final long toPacket;
        private final long toSend;
        private final long complete;

        private final long preparation;
        private final long packToProto;
        private final long processing;
        private final double rate;

        private Statistic(long start, long toProto, long toPacket, long toSend, long complete) {
            this.start = start;
            this.toProto = toProto;
            this.toPacket = toPacket;
            this.toSend = toSend;
            this.complete = complete;

            this.preparation = toProto - start;
            this.packToProto = toPacket - toProto;
            this.processing = complete - toSend;
            this.rate = 1_000_000_000D/this.processing;
        }

        private Statistic(long start, long toProto) {
            this(start, toProto, System.nanoTime(), -1, -1);
        }

        private Statistic withToSend() {
            return new Statistic(start, toProto, toPacket, System.nanoTime(), complete);
        }

        private Statistic withComplete() {
            return new Statistic(start, toProto, toPacket, toSend, System.nanoTime());
        }

        public long getPreparation() {
            return preparation;
        }

        public long getPackToProto() {
            return packToProto;
        }

        public long getProcessing() {
            return processing;
        }

        public double getRate() {
            return rate;
        }
    }

    private static class Packet {
        private final EventBatch batch;
        private final Statistic statistic;
        private Packet(EventBatch batch, long start, long toProto) {
            this.batch = batch;
            this.statistic = new Statistic(start, toProto);
        }
    }

    private static class TestConfirmation implements ManualAckDeliveryCallback.Confirmation {

        private final Lock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();
        private final Statistic origin;
        private final AtomicReference<Statistic> completed = new AtomicReference<>();

        private TestConfirmation(Statistic origin) {
            this.origin = origin.withToSend();
        }

        @Override
        public void confirm() {
            complete();
        }

        @Override
        public void reject() {
            complete();
        }

        public @Nonnull Statistic awaitComplete() throws InterruptedException {
            Statistic statistic = completed.get();
            if (statistic != null) {
                return statistic;
            }
            lock.lock();
            try {
                statistic = completed.get();
                if (statistic != null) {
                    return statistic;
                }
                condition.await();
                return requireNonNull(completed.get(), "Confirmation must have completed statistic");
            } finally {
                lock.unlock();
            }
        }

        private void complete() {
            lock.lock();
            try {
                if (!completed.compareAndSet(null, origin.withComplete())) {
                    throw new IllegalStateException("Confirmation can't be confirm / reject more then one");
                }
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}
