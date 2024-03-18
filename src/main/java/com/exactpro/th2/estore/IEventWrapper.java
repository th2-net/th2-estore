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
import com.exactpro.cradle.CradleEntitiesFactory;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventBatchToStoreBuilder;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatchOrBuilder;
import com.exactpro.th2.common.grpc.EventOrBuilder;
import com.exactpro.th2.common.util.StorageUtils;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public interface IEventWrapper {
    int count();
    int size();
    String id(); // FIXME: remove after debugging
    TestEventToStore get() throws CradleStorageException;

    static IEventWrapper wrap(TestEventSingleToStore event) {
        return new CradleEventSingleWrapper(event);
    }
    static IEventWrapper wrap(TestEventBatchToStore batch) {
        return new CradleEventBatchWrapper(batch);
    }
    static IEventWrapper wrap(CradleEntitiesFactory entitiesFactory, EventOrBuilder event) {
        return new ProtoEventSingleWrapper(entitiesFactory, event);
    }
    static IEventWrapper wrap(CradleEntitiesFactory entitiesFactory, EventBatchOrBuilder batch) {
        return new ProtoEventBatchWrapper(entitiesFactory, batch);
    }

    class CradleEventSingleWrapper implements IEventWrapper {
        private final TestEventSingleToStore event;

        private CradleEventSingleWrapper(TestEventSingleToStore event) {
            this.event = event;
        }

        @Override
        public int count() {
            return 1;
        }

        @Override
        public int size() {
            return event.getSize();
        }

        @Override
        public String id() {
            return event.getId().getId();
        }

        @Override
        public TestEventToStore get() {
            return event;
        }
    }
    class CradleEventBatchWrapper implements IEventWrapper {
        private final TestEventBatchToStore batch;

        private CradleEventBatchWrapper(TestEventBatchToStore batch) {
            this.batch = batch;
        }

        @Override
        public int count() {
            return batch.getTestEvents().size();
        }

        @Override
        public int size() {
            return batch.getBatchSize();
        }

        @Override
        public String id() {
            return batch.getTestEvents().iterator().next().getId().getId();
        }

        @Override
        public TestEventToStore get() {
            return batch;
        }
    }
    class ProtoEventSingleWrapper extends ProtoEventWrapper {
        private final EventOrBuilder event;
        private ProtoEventSingleWrapper(CradleEntitiesFactory entitiesFactory, EventOrBuilder event) {
            super(entitiesFactory);
            this.event = event;
        }

        @Override
        public int count() {
            return 1;
        }

        @Override
        public int size() {
            return event.getBody().size();
        }

        @Override
        public String id() {
            return event.getId().getId();
        }

        @Override
        protected TestEventToStore convert() throws CradleStorageException {
            return toCradleEvent(entitiesFactory, event);
        }
    }
    class ProtoEventBatchWrapper extends ProtoEventWrapper {
        private final EventBatchOrBuilder batch;

        private ProtoEventBatchWrapper(CradleEntitiesFactory entitiesFactory, EventBatchOrBuilder batch) {
            super(entitiesFactory);
            this.batch = batch;
        }

        @Override
        public int count() {
            return batch.getEventsCount();
        }

        @Override
        public int size() {
            return batch.getEventsList().stream()
                    .mapToInt(event -> event.getBody().size())
                    .sum();
        }

        @Override
        public String id() {
            return batch.getEvents(0).getId().getId();
        }

        @Override
        protected TestEventToStore convert() throws CradleStorageException {
            return toCradleBatch(entitiesFactory, batch);
        }
    }

    abstract class ProtoEventWrapper implements IEventWrapper {
        protected final CradleEntitiesFactory entitiesFactory;
        private final Lock lock = new ReentrantLock();
        private volatile TestEventToStore value;

        protected ProtoEventWrapper(CradleEntitiesFactory entitiesFactory) {
            this.entitiesFactory = entitiesFactory;
        }

        public abstract int count();
        public abstract int size();
        public abstract String id(); // FIXME: remove after debugging
        public TestEventToStore get() throws CradleStorageException {
            if (value != null) {
                return value;
            }
            lock.lock();
            try {
                if (value != null) {
                    return value;
                }
                value = convert();
                return value;
            } finally {
                lock.unlock();
            }
        }
        protected abstract TestEventToStore convert() throws CradleStorageException;

        static TestEventSingleToStore toCradleEvent(CradleEntitiesFactory entitiesFactory, EventOrBuilder event) throws CradleStorageException {
            TestEventSingleToStoreBuilder builder = entitiesFactory
                    .testEventBuilder()
                    .id(ProtoUtil.toCradleEventID(event.getId()))
                    .name(event.getName())
                    .type(event.getType())
                    .success(ProtoUtil.isSuccess(event.getStatus()))
                    .content(event.getBody().toByteArray());
            event.getAttachedMessageIdsList().stream()
                    .map(ProtoUtil::toStoredMessageId)
                    .forEach(builder::message);
            if (event.hasParentId()) {
                builder.parentId(ProtoUtil.toCradleEventID(event.getParentId()));
            }
            if (event.hasEndTimestamp()) {
                builder.endTimestamp(StorageUtils.toInstant(event.getEndTimestamp()));
            }
            return builder.build();
        }

        static TestEventBatchToStore toCradleBatch(CradleEntitiesFactory entitiesFactory, EventBatchOrBuilder batch) throws CradleStorageException {
            TestEventBatchToStoreBuilder cradleEventBatch = entitiesFactory.testEventBatchBuilder()
                    .id(
                            new BookId(batch.getParentEventId().getBookName()),
                            batch.getParentEventId().getScope(),
                            StorageUtils.toInstant(ProtoUtil.getMinStartTimestamp(batch.getEventsList())),
                            Util.generateId()
                    )
                    .parentId(ProtoUtil.toCradleEventID(batch.getParentEventId()));
            for (Event protoEvent : batch.getEventsList()) {
                cradleEventBatch.addTestEvent(toCradleEvent(entitiesFactory, protoEvent));
            }
            return cradleEventBatch.build();
        }
    }
}

