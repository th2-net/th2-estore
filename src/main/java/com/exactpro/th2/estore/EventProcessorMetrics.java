/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.estore;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

public class EventProcessorMetrics {
    private static final Histogram HISTOGRAM_PERSISTENCE_LATENCY = Histogram
            .build("th2_estore_processor_persistence_latency", "Event persistence latency")
            .buckets(0.010, 0.020, 0.050, 0.100, 0.200, 0.300, 0.400, 0.500, 1.000, 1.500, 2.000, 2.500, 3.000, 4.000, 5.000, 10.000)
            .register();

    private static final Counter COUNTER_FAILURES = Counter
            .build("th2_estore_processor_failures", "Number of event processing failures").register();

    public void registerFailure() {
        COUNTER_FAILURES.inc();
    }

    public Histogram.Timer startMeasuringPersistenceLatency() {

        return HISTOGRAM_PERSISTENCE_LATENCY.startTimer();
    }
}
