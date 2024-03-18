/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.slf4j.spi.LoggingEventBuilder;

import static java.util.Objects.requireNonNull;

class LogCallBack implements Callback<IEventWrapper> {
    private final LoggingEventBuilder loggingEventBuilder;
    private final boolean loggerEnabled;

    public LogCallBack(@NotNull Logger logger, Level level) {
        this.loggingEventBuilder = requireNonNull(logger, "Logger can't be null")
                .atLevel(requireNonNull(level, "Level can't be null"));
        this.loggerEnabled = logger.isEnabledForLevel(level);
    }
    @Override
    public void onSuccess(IEventWrapper data) {
        if (loggerEnabled) {
            loggingEventBuilder.log("Stored the {} test event single/batch with error", data.id());
        }
    }

    @Override
    public void onFail(IEventWrapper data) {
        if (loggerEnabled) {
            loggingEventBuilder.log("Storing of the {} test event single/batch with error failed", data.id());
        }
    }
}
