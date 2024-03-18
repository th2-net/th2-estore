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

import com.exactpro.cradle.CradleEntitiesFactory;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import static com.exactpro.cradle.CoreStorageSettings.DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS;
import static com.exactpro.cradle.CradleStorage.DEFAULT_MAX_MESSAGE_BATCH_SIZE;
import static com.exactpro.cradle.CradleStorage.DEFAULT_MAX_TEST_EVENT_BATCH_SIZE;
import static com.exactpro.th2.common.utils.message.MessageUtilsKt.toTimestamp;
import static org.openjdk.jmh.annotations.Mode.Throughput;

@State(Scope.Benchmark)
public class EventWrapperBenchmark {
    private static final CradleEntitiesFactory FACTORY = new CradleEntitiesFactory(DEFAULT_MAX_MESSAGE_BATCH_SIZE, DEFAULT_MAX_TEST_EVENT_BATCH_SIZE, DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS);
    public static final String BOOK = "benchmark-book";
    private static final String SCOPE = "benchmark-scope";
    private static final String SESSION_ALIAS_PREFIX = "benchmark-alias-";
    private static final String EVENT_NAME_PREFIX = "benchmark-event-";
    private static final int CONTENT_SIZE = 500;
    private static final int EVENT_NUMBER = 100;
    private static final int SESSION_ALIAS_NUMBER = 5;
    private static final int MESSAGES_PER_DIRECTION = 2;
    @State(Scope.Thread)
    public static class EventBatchState {
        private EventBatch batch;
        @Setup
        public void init() {
            EventID parentId = EventID.newBuilder()
                    .setBookName(BOOK)
                    .setScope(SCOPE)
                    .setStartTimestamp(toTimestamp(Instant.now()))
                    .setId(UUID.randomUUID().toString())
                    .build();
            EventBatch.Builder batchBuilder = EventBatch.newBuilder()
                            .setParentEventId(parentId);

            int seqCounter = 0;
            for (int eventIndex = 0; eventIndex < EVENT_NUMBER; eventIndex++) {
                Event.Builder eventBuilder = Event.newBuilder()
                        .setId(EventID.newBuilder()
                                .setBookName(BOOK)
                                .setScope(SCOPE)
                                .setStartTimestamp(toTimestamp(Instant.now()))
                                .setId(UUID.randomUUID().toString()))
                        .setParentId(parentId)
                        .setName(EVENT_NAME_PREFIX + eventIndex)
                        .setBody(UnsafeByteOperations.unsafeWrap(RandomStringUtils.random(CONTENT_SIZE, true, true).getBytes()));

                for (int aliasIndex = 0; aliasIndex < SESSION_ALIAS_NUMBER; aliasIndex++) {
                    for (Direction direction : Set.of(Direction.FIRST, Direction.SECOND)) {
                        for (int msgIndex = 0; msgIndex < MESSAGES_PER_DIRECTION; msgIndex++) {
                            MessageID.Builder messageIdBuilder = MessageID.newBuilder()
                                    .setBookName(BOOK)
                                    .setDirection(direction)
                                    .setTimestamp(toTimestamp(Instant.now()))
                                    .setSequence(++seqCounter);
                            messageIdBuilder.getConnectionIdBuilder()
                                            .setSessionAlias(SESSION_ALIAS_PREFIX + aliasIndex);
                            eventBuilder.addAttachedMessageIds(messageIdBuilder.build());
                        }
                    }
                }
                batchBuilder.addEvents(eventBuilder.build());
            }
            batch = batchBuilder.build();
        }
    }

    @Benchmark
    @BenchmarkMode({Throughput})
    public void benchmarkToCradleBatch(EventBatchState state) throws CradleStorageException {
        IEventWrapper.ProtoEventWrapper.toCradleBatch(FACTORY, state.batch);
    }
}
