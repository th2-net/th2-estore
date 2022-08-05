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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestScheduledRetryableJob {
    private static final long SCHEDULE_TIME = 1234134343;
    @Test
    public void testNonRetryable() {
        ScheduledRetryableJob<Object> job = new ScheduledRetryableJob<>(SCHEDULE_TIME, 1, 10, null);
        assertEquals(SCHEDULE_TIME, job.getScheduledTime(), "Job scheduled time");
        assertEquals(1, job.getExecutionsLeft(), "Executions left");
        assertEquals(0, job.getExecutionsDone(), "Executions done");
    }

    @Test
    public void testRetryable() {
        ScheduledRetryableJob<Object> job = new ScheduledRetryableJob<>(SCHEDULE_TIME, 3, 10, null);
        assertEquals(SCHEDULE_TIME, job.getScheduledTime(), "Job scheduled time");
        assertEquals(3, job.getExecutionsLeft(), "Executions left");
        assertEquals(0, job.getExecutionsDone(), "Executions done");
    }

    @Test
    public void testRetryOf() {
        ScheduledRetryableJob<Object> job = new ScheduledRetryableJob<>(SCHEDULE_TIME, 5, 10, null);

        long newScheduleTime1 = SCHEDULE_TIME + 1213;
        ScheduledRetryableJob<Object> retriedJob1 = ScheduledRetryableJob.retryOf(job, newScheduleTime1);

        assertEquals(newScheduleTime1, retriedJob1.getScheduledTime(), "Job scheduled time");
        assertEquals(4, retriedJob1.getExecutionsLeft(), "Executions left");
        assertEquals(1, retriedJob1.getExecutionsDone(), "Executions done");

        long newScheduleTime2 = SCHEDULE_TIME + 3456;
        ScheduledRetryableJob<Object> retriedJob2 = ScheduledRetryableJob.retryOf(retriedJob1, newScheduleTime2);

        assertEquals(newScheduleTime2, retriedJob2.getScheduledTime(), "Job scheduled time");
        assertEquals(3, retriedJob2.getExecutionsLeft(), "Executions left");
        assertEquals(2, retriedJob2.getExecutionsDone(), "Executions done");
    }

    @Test
    public void testOrder() {
        ScheduledRetryableJob<Object> job1 = new ScheduledRetryableJob<>(SCHEDULE_TIME, 5, 10, null);
        ScheduledRetryableJob<Object> job2 = new ScheduledRetryableJob<>(SCHEDULE_TIME + 1234, 6, 7, null);

        assertEquals(-1, ScheduledRetryableJob.compareOrder(job1, job2));
        assertEquals(0, ScheduledRetryableJob.compareOrder(job1, job1));
        assertEquals(1, ScheduledRetryableJob.compareOrder(job2, job1));
    }

}
