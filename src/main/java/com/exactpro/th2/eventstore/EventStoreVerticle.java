/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.eventstore;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.store.common.Configuration;
import com.exactpro.th2.store.common.utils.AsyncHelper;
import io.reactivex.Completable;
import io.vertx.grpc.VertxServerBuilder;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.impl.AsyncResultCompletable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class EventStoreVerticle extends AbstractVerticle {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + '@' + hashCode());
    private final CradleManager cradleManager;
    private final Configuration config;

    public EventStoreVerticle(Configuration config) throws IOException {
        this.config = config;
        cradleManager = new CassandraCradleManager(new CassandraConnection(this.config.getCassandraConfig().getConnectionSettings()));
    }

    @Override
    public Completable rxStart() {
        return Completable.merge(Arrays.asList(
            initManager(),
            startService()
        ));
    }

    private Completable startService() {
        return AsyncResultCompletable.toCompletable(h ->
            VertxServerBuilder.forAddress(vertx.getDelegate(),
                "0.0.0.0",
                config.getPort())
                .addService(new ReportEventStoreService(cradleManager, vertx))
                .build().start(h)
        );
    }

    private Completable initManager() {
        return vertx.<Void>rxExecuteBlocking(AsyncHelper
            .createHandler(() -> {
                try {
                    cradleManager.init(config.getCradleInstanceName());
                } catch (CradleStorageException e) {
                    logger.error("could not init cradle manager: {}", e.getMessage(), e);
                }
            })).ignoreElement();
    }
}
