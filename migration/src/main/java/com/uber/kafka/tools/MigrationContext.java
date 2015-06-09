// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Context object for migration job. Used for coordination
 * of migrator and producer threads.
 *
 * This class is thread-safe.
 */
public class MigrationContext {

    private final AtomicReference<ErrorCode> errorCode;
    private final AtomicBoolean failed;
    private final Set<String> topicsWithCorruptOffset;
    private MigrationMetrics metrics;

    public MigrationContext() {
        this.errorCode = new AtomicReference<ErrorCode>(ErrorCode.NA);
        this.failed = new AtomicBoolean(false);
        this.topicsWithCorruptOffset = Sets.newHashSet();
    }

    /**
     * Returns true if migration job failed; false otherwise.
     */
    public boolean failed() {
        return failed.get();
    }

    public void setFailed() {
        this.setFailed(ErrorCode.UNKNOWN);
    }

    public void setFailed(ErrorCode errorCode) {
        Preconditions.checkNotNull(errorCode, "Error code can't be null");
        Preconditions.checkArgument(errorCode != ErrorCode.NA, "Unexpected error code");
        // Preserve the root error code
        this.errorCode.compareAndSet(ErrorCode.NA, errorCode);
        // Set failed last
        this.failed.set(true);
    }

    public ErrorCode getErrorCode() {
        return errorCode.get();
    }

    /**
     * Returns a set of Kafka 0.7 topics with corrupt offsets.
     */
    public Set<String> getTopicsWithCorruptOffset() {
        synchronized (topicsWithCorruptOffset) {
            return ImmutableSet.copyOf(topicsWithCorruptOffset);
        }
    }

    public void addTopicWithCorruptOffset(String topic) {
        synchronized (topicsWithCorruptOffset) {
            topicsWithCorruptOffset.add(topic);
        }
    }

    public synchronized MigrationMetrics getMetrics() {
        return metrics;
    }

    public synchronized void setMetrics(MigrationMetrics metrics) {
        this.metrics = metrics;
    }

}
