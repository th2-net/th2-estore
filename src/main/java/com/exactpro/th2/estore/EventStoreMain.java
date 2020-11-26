/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.CradleManager;
import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.schema.factory.CommonFactory;

public class EventStoreMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventStoreMain.class);

    public static void main(String[] args) {
        Deque<AutoCloseable> resources = new ConcurrentLinkedDeque<>();
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        configureShutdownHook(resources, lock, condition);
        try {
            CommonMetrics.setLiveness(true);
            CommonFactory factory = CommonFactory.createFromArguments(args);
            resources.add(factory);
            CradleManager cradleManager = factory.getCradleManager();
            resources.add(cradleManager::dispose);
            ReportRabbitMQEventStoreService store = new ReportRabbitMQEventStoreService(factory.getEventBatchRouter(), cradleManager);
            resources.add(store::dispose);
            store.start();
            CommonMetrics.setReadiness(true);
            LOGGER.info("Event storing started");
            awaitShutdown(lock, condition);
        } catch (InterruptedException e) {
            LOGGER.info("The main thread interrupted", e);
        } catch (Exception e) {
            LOGGER.error("Fatal error: {}", e.getMessage(), e);
            System.exit(1);
        }
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
                CommonMetrics.setReadiness(false);
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
                CommonMetrics.setLiveness(false);
                LOGGER.info("Shutdown end");
            }
        });
    }
}
