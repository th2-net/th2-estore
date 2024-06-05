/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

public class Configuration {
    private static final int DEFAULT_MAX_TASK_RETRIES = 1000000;
    private static final int DEFAULT_MAX_TASK_COUNT = 256;
    private static final long DEFAULT_RETRY_DELAY_BASEM_S = 5000;
    private static final int DEFAULT_PROCESSING_THREADS = Runtime.getRuntime().availableProcessors();

    private Integer maxTaskCount;
    private Long maxTaskDataSize;
    private Integer maxRetryCount;
    private Long retryDelayBase;
    private Integer processingThreads;

    public Long getMaxTaskDataSize() {
        return maxTaskDataSize == null ? defaultMaxDataSize() : maxTaskDataSize;
    }

    public Integer getMaxTaskCount() {
        return maxTaskCount == null ? DEFAULT_MAX_TASK_COUNT : maxTaskCount;
    }

    public Integer getMaxRetryCount() {
        return maxRetryCount == null ? DEFAULT_MAX_TASK_RETRIES : maxRetryCount;
    }

    public Long getRetryDelayBase() {
        return retryDelayBase == null ? DEFAULT_RETRY_DELAY_BASEM_S : retryDelayBase;
    }

    public int getProcessingThreads() {
        return processingThreads == null ? DEFAULT_PROCESSING_THREADS : processingThreads;
    }

    public Configuration() {
        this(DEFAULT_MAX_TASK_COUNT, DEFAULT_MAX_TASK_RETRIES, DEFAULT_RETRY_DELAY_BASEM_S,
                defaultMaxDataSize(), DEFAULT_PROCESSING_THREADS);
    }

    @JsonCreator
    public Configuration(Integer maxTaskCount, Integer maxTaskRetries, Long taskRetryDelayBase,
                         Long maxTaskDataSize, Integer processingThreads) {
        this.maxTaskCount = maxTaskCount;
        this.maxRetryCount = maxTaskRetries;
        this.retryDelayBase = taskRetryDelayBase;
        this.maxTaskDataSize = maxTaskDataSize;
        this.processingThreads = processingThreads;
        validate();
    }

    public void validate() {
        if (maxTaskCount <= 1) throw new IllegalArgumentException("'maxTaskCount' should be >=1. Actual: " + maxTaskCount);
        if (maxTaskDataSize <= 1) throw new IllegalArgumentException("'maxTaskDataSize' should be >=1. Actual: " + maxTaskDataSize);
        if (maxRetryCount <= 0) throw new IllegalArgumentException("'maxRetryCount' should be >=0. Actual: " + maxRetryCount);
        if (retryDelayBase <= 1) throw new IllegalArgumentException("'retryDelayBase' should be >=1. Actual: " + retryDelayBase);
        if (processingThreads <= 1) throw new IllegalArgumentException("'processingThreads' should be >=1. Actual: " + processingThreads);
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
                "}";
    }
}