/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.time.Instant;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.exactpro.th2.common.utils.ExecutorServiceUtilsKt.shutdownGracefully;

public class EventStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStore.class);
    private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("error-collector-%d").build();

    public static void main(String[] args) {
        Deque<AutoCloseable> resources = new ConcurrentLinkedDeque<>();
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        configureShutdownHook(resources, lock, condition);
        try {
            CommonMetrics.LIVENESS_MONITOR.enable();

            CommonFactory factory = CommonFactory.createFromArguments(args);
            resources.add(factory);

            Configuration config = factory.getCustomConfiguration(Configuration.class);
            if (config == null) {
                config = new Configuration();
            }

            LOGGER.info("Effective configuration:\n{}", config);

            CradleManager cradleManager = factory.getCradleManager();
            resources.add(cradleManager);

            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);
            resources.add(() -> shutdownGracefully(executor, 5, TimeUnit.SECONDS));

            CradleStorage storage = cradleManager.getStorage();

            ErrorCollector errorCollector = new ErrorCollector(executor, storage.getEntitiesFactory());
            resources.add(errorCollector);

            EventPersistor persistor = new EventPersistor(errorCollector, config, storage);
            resources.add(persistor);
            persistor.start();

            StoredTestEventId rootEventId = createAndStoreRootEvent(persistor, storage.getEntitiesFactory(), factory.getBoxConfiguration());
            errorCollector.init(persistor, rootEventId);

            EventProcessor processor = new EventProcessor(errorCollector, factory.getEventBatchRouter(),
                    storage.getEntitiesFactory(),
                    persistor);
            resources.add(processor);
            processor.start();

            CommonMetrics.READINESS_MONITOR.enable();

            LOGGER.info("Event storing started");
            awaitShutdown(lock, condition);
        } catch (InterruptedException e) {
            LOGGER.info("The main thread interrupted", e);
        } catch (Exception e) {
            LOGGER.error("Fatal error: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static StoredTestEventId createAndStoreRootEvent(EventPersistor persistor, CradleEntitiesFactory entitiesFactory, BoxConfiguration boxConfiguration) throws CradleStorageException {
        Instant now = Instant.now();
        StoredTestEventId rootEventId = new StoredTestEventId(new BookId(boxConfiguration.getBookName()),
                boxConfiguration.getBoxName(),
                now,
                Util.generateId());
        TestEventSingleToStore eventToStore = entitiesFactory.testEventBuilder()
                .id(rootEventId)
                .name(boxConfiguration.getBoxName() + " " + now)
                .type("Microservice")
                .success(true)
                .endTimestamp(now)
                .content(new byte[0])
                .build();
        persistor.persist(eventToStore, new LogCallBack(LOGGER, Level.INFO));
        return rootEventId;
    }

    private static void awaitShutdown(ReentrantLock lock, Condition condition) throws InterruptedException {
        try {
            lock.lock();
            LOGGER.info("Wait to shutdown");
            condition.await();
            LOGGER.info("App has been shut down");
        } finally {
            lock.unlock();
        }
    }

    private static void configureShutdownHook(Deque<AutoCloseable> resources, ReentrantLock lock, Condition condition) {
        Runtime.getRuntime().addShutdownHook(new Thread("Shutdown hook") {
            @Override
            public void run() {
                LOGGER.info("Shutdown start");
                CommonMetrics.READINESS_MONITOR.disable();
                try {
                    lock.lock();
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }

                resources.descendingIterator().forEachRemaining(resource -> {
                    try {
                        resource.close();
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
                CommonMetrics.LIVENESS_MONITOR.disable();
                LOGGER.info("Shutdown end");
            }
        });
    }
}
