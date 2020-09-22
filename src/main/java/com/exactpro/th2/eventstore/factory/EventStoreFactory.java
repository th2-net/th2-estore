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

package com.exactpro.th2.eventstore.factory;

import java.nio.file.Path;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.exactpro.th2.eventstore.router.EventBatchRouter;
import com.exactpro.th2.infra.grpc.EventBatch;
import com.exactpro.th2.schema.factory.CommonFactory;
import com.exactpro.th2.schema.message.MessageRouter;

public class EventStoreFactory extends CommonFactory {

    private static final Path CONFIG_DEFAULT_PATH = Path.of("/var/th2/config/");

    private static final String RABBIT_MQ_FILE_NAME = "rabbitMQ.json";
    private static final String ROUTER_MQ_FILE_NAME = "mq.json";
    private static final String ROUTER_GRPC_FILE_NAME = "grpc.json";
    private static final String CRADLE_FILE_NAME = "cradle.json";
    private static final String CUSTOM_FILE_NAME = "custom.json";

    public EventStoreFactory(Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path dictionariesDir) {
        super(rabbitMQ, routerMQ, routerGRPC, cradle, custom, dictionariesDir);
    }

    public EventStoreFactory(){
        super();
    }

    public MessageRouter<EventBatch> getEventBatchRouter() {
        EventBatchRouter router = new EventBatchRouter();
        router.init(getRabbitMqConfiguration(), getMessageRouterConfiguration());
        return router;
    }

    public int getGrpcPort() {
        return getGrpcRouterConfiguration().getServerConfiguration().getPort();
    }

    public String getGrpcHost() {
        return getGrpcRouterConfiguration().getServerConfiguration().getHost();
    }

    public static EventStoreFactory createFromArguments(String... args) throws ParseException {
        Options options = new Options();

        options.addOption(new Option(null, "rabbitConfiguration", true, null));
        options.addOption(new Option(null, "messageRouterConfiguration", true, null));
        options.addOption(new Option(null, "grpcRouterConfiguration", true, null));
        options.addOption(new Option(null, "cradleConfiguration", true, null));
        options.addOption(new Option(null, "customConfiguration", true, null));
        options.addOption(new Option(null, "dictionariesDir", true, null));
        options.addOption(new Option("c", "configs", true, null));

        CommandLine cmd = new DefaultParser().parse(options, args);

        String configs = cmd.getOptionValue("configs");

        return new EventStoreFactory(
                calculatePath(cmd.getOptionValue("rabbitConfiguration"), configs, RABBIT_MQ_FILE_NAME),
                calculatePath(cmd.getOptionValue("messageRouterConfiguration"), configs, ROUTER_MQ_FILE_NAME),
                calculatePath(cmd.getOptionValue("grpcRouterConfiguration"), configs, ROUTER_GRPC_FILE_NAME),
                calculatePath(cmd.getOptionValue("cradleConfiguration"), configs, CRADLE_FILE_NAME),
                calculatePath(cmd.getOptionValue("customConfiguration"), configs, CUSTOM_FILE_NAME),
                calculatePath(cmd.getOptionValue("dictionariesDir"), configs)
        );
    }


    private static Path calculatePath(String path, String configsPath) {
        return path != null ? Path.of(path) : (configsPath != null ? Path.of(configsPath) : CONFIG_DEFAULT_PATH);
    }

    private static Path calculatePath(String path, String configsPath, String fileName) {
        return path != null ? Path.of(path) : (configsPath != null ? Path.of(configsPath, fileName) : CONFIG_DEFAULT_PATH.resolve(fileName));
    }


}
