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
package com.exactpro.th2.eventstore;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.eventstore.factory.EventStoreFactory;
import com.exactpro.th2.schema.cradle.CradleConfiguration;
import com.exactpro.th2.store.common.configuration.StoreConfiguration;
import com.exactpro.th2.store.common.utils.AsyncHelper;

import io.reactivex.Completable;
import io.vertx.grpc.VertxServerBuilder;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.impl.AsyncResultCompletable;

public class EventStoreVerticle extends AbstractVerticle {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + '@' + hashCode());
    private final CradleManager cradleManager;
    private final EventStoreFactory factory;

    public EventStoreVerticle(EventStoreFactory factory) throws IOException {
        CradleConfiguration cradleConfiguration = factory.getCradleConfiguration();

        CassandraConnectionSettings cassandraConnectionSettings = new CassandraConnectionSettings(
                cradleConfiguration.getDataCenter(),
                cradleConfiguration.getHost(),
                cradleConfiguration.getPort(),
                cradleConfiguration.getKeyspace());

        if (StringUtils.isNotEmpty(cradleConfiguration.getUsername())) {
            cassandraConnectionSettings.setUsername(cradleConfiguration.getUsername());
        }

        if (StringUtils.isNotEmpty(cradleConfiguration.getPassword())) {
            cassandraConnectionSettings.setPassword(cradleConfiguration.getPassword());
        }

        this.cradleManager = new CassandraCradleManager(new CassandraConnection(cassandraConnectionSettings));
        this.factory = factory;
    }

    @Override
    public Completable rxStart() {
        return Completable.merge(Arrays.asList(
            initManager(),
            startGrpcService(),
            startRabbitMQService()
        ));
    }

    private Completable startRabbitMQService() {
        return vertx.rxExecuteBlocking(AsyncHelper.createHandler( () -> {
            try {
                ReportRabbitMQEventStoreService store = new ReportRabbitMQEventStoreService(factory.getEventBatchRouter(), cradleManager);
                store.start();
            } catch (Exception e) {
                logger.error("Can not start rabbit mq event store", e);
            }
        })).ignoreElement();
    }

    private Completable startGrpcService() {
        return AsyncResultCompletable.toCompletable(h ->
            VertxServerBuilder.forAddress(vertx.getDelegate(),
                factory.getGrpcHost(),
                factory.getGrpcPort())
                .addService(new ReportEventStoreService(cradleManager, vertx))
                .build().start(h)
        );
    }

    private Completable initManager() {
        return vertx.<Void>rxExecuteBlocking(AsyncHelper
            .createHandler(() -> {
                try {
                    cradleManager.init(factory.getCustomConfiguration(StoreConfiguration.class).getCradleInstanceName());
                } catch (CradleStorageException e) {
                    logger.error("could not init cradle manager: {}", e.getMessage(), e);
                }
            })).ignoreElement();
    }
}
