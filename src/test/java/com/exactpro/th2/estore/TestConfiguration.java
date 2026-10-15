/*
 * Copyright 2026 Exactpro (Exactpro Systems Limited)
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestConfiguration {

    @Test
    @DisplayName("#164: default max task data size is half of max heap size")
    public void testDefaultMaxTaskDataSize() {
        long expected = Runtime.getRuntime().maxMemory() / 2;

        assertEquals(expected, new Configuration().getMaxTaskDataSize(), "default constructor");
        assertEquals(expected, new Configuration(null, null, null, null, null, null).getMaxTaskDataSize(), "json creator");
    }

    @Test
    @DisplayName("configured max task data size is used")
    public void testConfiguredMaxTaskDataSize() {
        assertEquals(1_000L, new Configuration(null, 1_000L, null, null, null, null).getMaxTaskDataSize());
    }
}
