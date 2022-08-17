/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import java.util.concurrent.atomic.AtomicLong;

public class Metrics {
    private final AtomicLong value;
    private long lastQueryValue;
    private long lastQueryTime;

    public Metrics() {
        value = new AtomicLong();
        lastQueryTime = System.nanoTime();
    }

    public void addValue(long delta) {
        value.addAndGet(delta);
    }

    synchronized public double getCurrentSpeed() {
        long v = value.get();
        long t = System.nanoTime();

        long deltaV = v - lastQueryValue;
        long deltaT = t - lastQueryTime;

        lastQueryValue = v;
        lastQueryTime = t;

        return Double.valueOf(deltaV * 1_000_000_000) / deltaT;
    }

}
