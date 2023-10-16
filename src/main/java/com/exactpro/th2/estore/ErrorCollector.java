/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.th2.common.event.IBodyData;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("unused")
public class ErrorCollector implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorCollector.class);
    private static final Callback<TestEventToStore> PERSIST_CALL_BACK = new LogCallBack(LOGGER, Level.TRACE);
    private static final ThreadLocal<ObjectMapper> OBJECT_MAPPER = ThreadLocal.withInitial(() ->
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    // otherwise, type supported by JavaTimeModule will be serialized as array of date component
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                    .setSerializationInclusion(NON_NULL));
    private static final Persistor<TestEventToStore> DYMMY_PERSISTOR = new DymmyPersistor();
    private final ScheduledFuture<?> drainFuture;
    private final CradleEntitiesFactory entitiesFactory;
    private final Lock lock = new ReentrantLock();
    private volatile StoredTestEventId rootEvent;
    private volatile Persistor<TestEventToStore> persistor = DYMMY_PERSISTOR;
    private Map<String, ErrorMetadata> errors = new HashMap<>();

    public ErrorCollector(@NotNull ScheduledExecutorService executor,
                          @NotNull CradleEntitiesFactory entitiesFactory,
                          long period,
                          @NotNull TimeUnit unit) {
        this.entitiesFactory = requireNonNull(entitiesFactory, "Entities factory can't be null");
        requireNonNull(unit, "Unit can't be null");
        this.drainFuture = requireNonNull(executor, "Executor can't be null")
                .scheduleAtFixedRate(this::drain, period, period, unit);
    }

    public ErrorCollector(@NotNull ScheduledExecutorService executor,
                          @NotNull CradleEntitiesFactory entitiesFactory) {
        this(executor, entitiesFactory, 1, TimeUnit.MINUTES);
    }

    public void init(@NotNull Persistor<TestEventToStore> persistor, StoredTestEventId rootEvent) {
        this.persistor = requireNonNull(persistor, "Persistor factory can't be null");
        this.rootEvent = requireNonNull(rootEvent, "Root event id can't be null");
    }

    /**
     * Log error and call the {@link #collect(String)}} method
     * @param error is used as key identifier. Avoid put a lot of unique values
     */
    public void collect(Logger logger, String error, Throwable cause) {
        logger.error(error, cause);
        collect(error);
    }

    /**
     * @param error is used as key identifier. Avoid put a lot of unique values
     */
    public void collect(String error) {
        lock.lock();
        try {
            errors.compute(error, (key, metadata) -> {
                if (metadata == null) {
                    return new ErrorMetadata();
                }
                metadata.inc();
                return metadata;
            });
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        drainFuture.cancel(true);
        drain();
    }

    private void drain() {
        if (rootEvent == null || persistor == DYMMY_PERSISTOR) {
            LOGGER.warn( "{} isn't initialised", ErrorCollector.class.getSimpleName());
        }

        try {
            Map<String, ErrorMetadata> map = clear();
            if (map.isEmpty()) { return; }

            Instant now = Instant.now();
            TestEventSingleToStore eventToStore = entitiesFactory.testEventBuilder()
                    .id(new StoredTestEventId(rootEvent.getBookId(), rootEvent.getScope(), now, Util.generateId()))
                    .name("estore internal problem(s): " + calculateTotalQty(map.values()))
                    .type("InternalError")
                    .success(false)
                    .parentId(rootEvent)
                    .endTimestamp(now)
                    // Content wrapped to list to use the same format as mstore
                    .content(OBJECT_MAPPER.get().writeValueAsBytes(List.of(new BodyData(map))))
                    .build();

            persistor.persist(eventToStore, PERSIST_CALL_BACK);
        } catch (Exception e) {
            LOGGER.error("Drain events task failure", e);
        }
    }

    private Map<String, ErrorMetadata> clear() {
        lock.lock();
        try {
            Map<String, ErrorMetadata> result = errors;
            errors = new HashMap<>();
            return result;
        } finally {
            lock.unlock();
        }
    }

    private static int calculateTotalQty(Collection<ErrorMetadata> errors) {
        return errors.stream()
                .map(ErrorMetadata::getQuantity)
                .reduce(0, Integer::sum);
    }

    private static class BodyData implements IBodyData {
        private final Map<String, ErrorMetadata> errors;
        @JsonCreator
        private BodyData(Map<String, ErrorMetadata> errors) {
            this.errors = errors;
        }
        public Map<String, ErrorMetadata> getErrors() {
            return errors;
        }
    }

    private static class ErrorMetadata {
        private final Instant firstDate = Instant.now();
        private Instant lastDate;
        private int quantity = 1;

        public void inc() {
            quantity += 1;
            lastDate = Instant.now();
        }

        public Instant getFirstDate() {
            return firstDate;
        }

        public Instant getLastDate() {
            return lastDate;
        }

        public void setLastDate(Instant lastDate) {
            this.lastDate = lastDate;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }
    }

    private static class DymmyPersistor implements Persistor<TestEventToStore> {

        @Override
        public void persist(TestEventToStore data, Callback<TestEventToStore> callback) {
            LOGGER.warn( "{} isn't initialised", ErrorCollector.class.getSimpleName());
        }
    }
}
