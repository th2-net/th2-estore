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

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BlockingRetryableJobQueue<V> {

    private final int maxJobCount;
    private final RetryScheduler scheduler;
    private final Queue<ScheduledRetryableJob<V>> queue;

    private volatile long capacityLeft;
    private final Lock lock;
    private final Condition nonEmpty;
    private final Condition notFull;

    /**
     * <P>
     * Creates new queue with given maximum job capacity, maximum total job data capacity and retry scheduler.
     * Queue does not allow new jobs to exceed capacity limitations, but actual capacities might be exceeded
     * if jobs are resubmitted for retry.
     * </P>
     * <P>
     * Job extraction order is determined by their schedule time, that is job with earlier schedule time will be extracted before
     * the job with later schedule time. For equal schedule times no particular order is guarantied
     *</P>
     * @param maxJobCount   Maximum number of jobs that can be submitted before blocking.
     *                      This parameter has effect on submit() method which will be blocked if capacity
     *                      limitations are reached.
     * @param maxDataSize   Maximum cumulative data size of all jobs in the queue before blocking.
     *                      This parameter has effect on submit() method which will be blocked if capacity
     *                      limitations are reached.
     * @param scheduler     RetryScheduler that is called to compute retry delay time when submitting job for retry.
     *                      In case of <I>null</I> value is provided, 0 delay will be used.
     *
     */
    public BlockingRetryableJobQueue(int maxJobCount, long maxDataSize, RetryScheduler scheduler) {
        this.maxJobCount = maxJobCount;
        this.scheduler = (scheduler != null) ? scheduler : (unused) -> 0;
        this.capacityLeft = maxDataSize;

        queue = new PriorityQueue<>(ScheduledRetryableJob::compareOrder);
        lock = new ReentrantLock();
        nonEmpty = lock.newCondition();
        notFull = lock.newCondition();
    }


    /**
     * Submits job to the queue.<BR>
     * Blocks until available capacity requirements are met, that is queue has space for this job by
     * the count and cumulative data size of submitted jobs
     *
     * @param job   Job to be added to the queue
     */
    public void submit(ScheduledRetryableJob<V> job) {
        lock.lock();
        try {
            while (true) {
                if (capacityLeft >= job.getPayloadSize() && queue.size() < maxJobCount) {
                    offer(job);
                    break;
                } else {
                    notFull.awaitUninterruptibly();
                }
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * Submits job for retry to the queue.<BR>
     * New job object will be created with schedule time calculated by RetryScheduler provided when queue was created.
     * If job retry limit was reached than it will not be added to the queue.
     *
     * This method does not block and does not regard queue capacity limitations so after this call queue's capacity
     * limitations might be exceeded.
     *
     * @param job   Job to be resubmitted to the queue
     * @return true  if job was resubmitted to queue, false if due to retry limit was reached
     */
    public boolean retry(ScheduledRetryableJob<V> job) {
        if (job.getExecutionsLeft() == 0)
            return false;

        ScheduledRetryableJob<V> retriedJob = ScheduledRetryableJob.retryOf(
                job,
                System.nanoTime() + scheduler.nextRetry(job.getExecutionsDone()));

        lock.lock();
        try {
            offer(retriedJob);
        } finally {
            lock.unlock();
        }
        return true;
    }


    /**<P>
     *     Returns job with earliest schedule time. Blocks until any job is available in the queue.<BR>
     *     This method does not take into consideration job's schedule time, so it might return job that is scheduled
     *     in the future. Use <I>awaitScheduled()</I> method to wait until there is actual job that can be executed
     *     according the schedule.
     * </P>
     * <P>
     *     No order is guarantied if multiple jobs have same schedule time
     *</P>
     * @return  Job with earliest schedule time.
     */
    public ScheduledRetryableJob<V> take() {
        lock.lock();
        try {
            while (true) {
                if (queue.size() > 0) {
                    return poll();
                } else
                    nonEmpty.awaitUninterruptibly();
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * <P>
     *     Waits and returns job with earliest schedule time that can be immediately executed. Blocks until such job is available.
     *     This method will block even when queue is not empty, if all jobs are scheduled for execution in future.
     * </P>
     * <P>
     *     No order is guarantied if multiple jobs have same schedule time
     * </P>
     *
     * @return  Job with earliest schedule time.
     * @throws InterruptedException if method call was interrupted
     */
    public ScheduledRetryableJob<V> awaitScheduled() throws InterruptedException {
        lock.lock();
        try {
            while (true) {
                if (queue.size() == 0)
                    nonEmpty.awaitUninterruptibly();
                else {
                    ScheduledRetryableJob<V> job = queue.peek();
                    long now = System.nanoTime();
                    if (now < job.getScheduledTime())
                        nonEmpty.awaitNanos(job.getScheduledTime() - now);
                    else
                        return job;
                }
            }
        } finally {
            lock.lock();
        }
    }


    private void offer(ScheduledRetryableJob<V> job) {
        queue.offer(job);
        capacityLeft -= job.getPayloadSize();
        nonEmpty.signalAll();
    }


    private ScheduledRetryableJob<V> poll() {
        ScheduledRetryableJob<V> job = queue.poll();
        capacityLeft += job.getPayloadSize();
        notFull.signalAll();
        return job;
    }
}
