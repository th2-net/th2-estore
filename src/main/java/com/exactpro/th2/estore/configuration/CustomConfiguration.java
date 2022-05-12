package com.exactpro.th2.estore.configuration;

public class CustomConfiguration {
    private int maxParallelEventCount = 50;

    public int getMaxParallelEventCount() {
        return maxParallelEventCount;
    }

    public void setMaxParallelEventCount(int maxParallelEventCount) {
        this.maxParallelEventCount = maxParallelEventCount;
    }
}
