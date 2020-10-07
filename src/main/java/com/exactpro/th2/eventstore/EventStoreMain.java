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

import static java.util.Objects.requireNonNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.metrics.CommonMetrics;
import com.exactpro.th2.schema.factory.CommonFactory;

import io.vertx.core.Vertx;

public class EventStoreMain {

    private final static Logger LOGGER = LoggerFactory.getLogger(EventStoreMain.class);

    public static void main(String[] args) {
        try {
            CommonMetrics.setLiveness(true);
            CommonFactory factory = CommonFactory.createFromArguments(args);

            int grpcPort = requireNonNull(
                    requireNonNull(factory.getGrpcRouterConfiguration(), "Configuration for grpc router can not be null")
                            .getServerConfiguration(), "Configuration for grpc server can not be null")
                    .getPort();

            Vertx vertx = Vertx.vertx();
            EventStoreVerticle eventStoreVerticle = new EventStoreVerticle(factory, grpcPort);
            vertx.deployVerticle(eventStoreVerticle);
            LOGGER.info("event store started on {} port", grpcPort);
        } catch (Exception e) {
            CommonMetrics.setLiveness(false);
            CommonMetrics.setReadiness(false);
            LOGGER.error("fatal error: {}", e.getMessage(), e);
            System.exit(-1);
        }
    }
}
