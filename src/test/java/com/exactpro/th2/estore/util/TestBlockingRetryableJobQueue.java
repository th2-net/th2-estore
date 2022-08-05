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

public class TestBlockingRetryableJobQueue {

    @Test
    public void testJobExtractionOrder() {

        long startTime = 1278417364718L;

        ScheduledRetryableJob<Object> job1 = new ScheduledRetryableJob<>(startTime, 1, 100, null);
        ScheduledRetryableJob<Object> job2 = new ScheduledRetryableJob<>(startTime + 1234, 2, 100, null);
        ScheduledRetryableJob<Object> job3 = new ScheduledRetryableJob<>(startTime + 5678, 3, 100, null);
        BlockingRetryableJobQueue<Object> queue = new BlockingRetryableJobQueue<>(Integer.MAX_VALUE, Long.MAX_VALUE, null);
        queue.submit(job2);
        queue.submit(job3);
        queue.submit(job1);

        assertEquals(job1, queue.take());
        assertEquals(job2, queue.take());
        assertEquals(job3, queue.take());
    }
}