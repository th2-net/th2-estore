/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Configuration {
    private static final int DEFAULT_MAX_TASK_RETRIES = 1000000;
    private static final int DEFAULT_MAX_TASK_COUNT = 128;
    private static final long DEFAULT_RETRY_DELAY_BASEM_S = 5000;
    private static final int DEFAULT_PROCESSING_THREADS = 1;
    private static final long DEFAULT_WAIT_TIMEOUT = 5000L;

    private final int maxTaskCount;
    private final long maxTaskDataSize;
    private final int maxRetryCount;
    private final long retryDelayBase;
    private final int processingThreads;
    private final long persisotrTerminationTimeout;

    public Configuration() {
        this(DEFAULT_MAX_TASK_COUNT, defaultMaxDataSize(), DEFAULT_MAX_TASK_RETRIES,
                DEFAULT_RETRY_DELAY_BASEM_S, DEFAULT_PROCESSING_THREADS, DEFAULT_WAIT_TIMEOUT);
    }

    @JsonCreator
    public Configuration(
            @JsonProperty("maxTaskCount") Integer maxTaskCount,
            @JsonProperty("maxTaskDataSize") Long maxTaskDataSize,
            @JsonProperty("maxRetryCount") Integer maxRetryCount,
            @JsonProperty ("retryDelayBase") Long retryDelayBase,
            @JsonProperty("processingThreads") Integer processingThreads,
            @JsonProperty("persisotrTerminationTimeout") Long persisotrTerminationTimeout
    ) {
        this.maxTaskCount = maxTaskCount == null ? DEFAULT_MAX_TASK_COUNT : maxTaskCount;
        this.maxTaskDataSize = maxTaskDataSize == null ? defaultMaxDataSize() : maxTaskDataSize;
        this.maxRetryCount = maxRetryCount == null ? DEFAULT_MAX_TASK_RETRIES : maxRetryCount;
        this.retryDelayBase = retryDelayBase == null ? DEFAULT_RETRY_DELAY_BASEM_S : retryDelayBase;
        this.processingThreads = processingThreads == null ? DEFAULT_PROCESSING_THREADS : processingThreads;
        this.persisotrTerminationTimeout = persisotrTerminationTimeout == null ? DEFAULT_WAIT_TIMEOUT : persisotrTerminationTimeout;

        if (this.maxTaskCount < 1) throw new IllegalArgumentException("'maxTaskCount' should be >=1. Actual: " + maxTaskCount);
        if (this.maxTaskDataSize < 1) throw new IllegalArgumentException("'maxTaskDataSize' should be >=1. Actual: " + maxTaskDataSize);
        if (this.maxRetryCount < 0) throw new IllegalArgumentException("'maxRetryCount' should be >=0. Actual: " + maxRetryCount);
        if (this.retryDelayBase < 1) throw new IllegalArgumentException("'retryDelayBase' should be >=1. Actual: " + retryDelayBase);
        if (this.processingThreads < 1) throw new IllegalArgumentException("'processingThreads' should be >=1. Actual: " + processingThreads);
        if (this.persisotrTerminationTimeout < 0) throw new IllegalArgumentException("'persisotrTerminationTimeout' should be >=0. Actual: " + persisotrTerminationTimeout);
    }

    public int getMaxTaskCount() {
        return maxTaskCount;
    }

    public long getMaxTaskDataSize() {
        return maxTaskDataSize;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public long getRetryDelayBase() {
        return retryDelayBase;
    }

    public int getProcessingThreads() {
        return processingThreads;
    }

    public long getPersisotrTerminationTimeout() {
        return persisotrTerminationTimeout;
    }

    private static long defaultMaxDataSize() {
        return Runtime.getRuntime().totalMemory() / 2;
    }

    @Override
    public String toString() {
        String PREFIX = "  \"";
        String SUFFIX = ",\n";
        String SUFFIX_LAST = "\n";
        String SEP = "\" : ";
        return "{\n" +
                PREFIX + "maxTaskCount" + SEP + getMaxTaskCount() + SUFFIX +
                PREFIX + "maxTaskDataSize" + SEP + getMaxTaskDataSize() + SUFFIX +
                PREFIX + "maxRetryCount" + SEP + getMaxRetryCount() + SUFFIX +
                PREFIX + "retryDelayBase" + SEP + getRetryDelayBase() + SUFFIX +
                PREFIX + "processingThreads" + SEP + getProcessingThreads() + SUFFIX_LAST +
                PREFIX + "persisotrTerminationTimeout" + SEP + getPersisotrTerminationTimeout() + SUFFIX_LAST +
                "}";
    }
}