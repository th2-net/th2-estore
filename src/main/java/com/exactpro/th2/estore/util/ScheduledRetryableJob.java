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

package com.exactpro.th2.estore.util;

public class ScheduledRetryableJob<V> {
    private final long scheduledTime;
    private final int  executionsLeft;
    private final int  executionsDone;
    private final long payloadSize;
    private final V    payload;

    private ScheduledRetryableJob(long scheduledTime, int maxExecutions, int executionsDone, long payloadSize, V payload) {
        if (maxExecutions <= 0)
            throw new IllegalArgumentException("Number of max executions must be positive");

        if (payloadSize < 0)
            throw new IllegalArgumentException("Payload size can not be negative");

        this.scheduledTime = scheduledTime;
        this.executionsLeft = maxExecutions;
        this.executionsDone = executionsDone;
        this.payloadSize = payloadSize;
        this.payload = payload;
    }


    public ScheduledRetryableJob(long scheduledTime, int maxExecutions, long payloadSize, V payload) {
        this(scheduledTime, maxExecutions, 0, payloadSize, payload);
    }


    public static<V> ScheduledRetryableJob<V> retryOf(ScheduledRetryableJob<V> job, long scheduledTime) {
        if (job.executionsLeft == 0)
            throw new IllegalStateException("No more retries are possible");

        return new ScheduledRetryableJob<>(scheduledTime,
                job.executionsLeft - 1,
                job.executionsDone + 1,
                job.payloadSize,
                job.payload);
    }


    public int getExecutionsLeft() {
        return executionsLeft;
    }

    public long getScheduledTime() {
        return scheduledTime;
    }

    public int getExecutionsDone() {
        return executionsDone;
    }

    public long getPayloadSize() {
        return payloadSize;
    }

    public V getPayload() {
        return payload;
    }

    public static<V> int compareOrder(ScheduledRetryableJob<V> job1, ScheduledRetryableJob<V> job2) {
        return Long.compare(job1.scheduledTime, job2.scheduledTime);
    }

    @Override
    public String toString() {
        return String.format("%s {scheduledTime: %d, executionsLeft: %d, executionsDone: %d, payloadSize: %d, payload: %s}",
                ScheduledRetryableJob.class.getSimpleName(),
                scheduledTime,
                executionsLeft,
                executionsDone,
                payloadSize,
                payload);
    }
}
