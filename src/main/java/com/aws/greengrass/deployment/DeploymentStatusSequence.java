/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.deployment;

import com.aws.greengrass.config.Node;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.util.Coerce;

/**
 * Allocates the monotonically increasing sequence numbers used as keys of persisted deployment status records.
 *
 * <p>Sequence numbers are not derived from the device clock: an offline device may have its clock corrected or moved
 * backwards, so wall-clock values cannot order records. The counter is persisted in its own topic, a sibling of
 * {@link DeploymentStatusKeeper#PROCESSED_DEPLOYMENTS_TOPICS}, because that topic's children are removed after a
 * successful replay - a counter stored there would reset and hand out numbers lower than records still pending.</p>
 *
 * <p>The counter is persisted (and flushed to the transaction log) <em>before</em> the record that uses the number is
 * written, so a crash can only leave a harmless hole in the sequence, never a record whose number the counter has not
 * reached yet. On first use the counter is initialised to at least the highest sequence found among persisted records,
 * which recovers from a record that was written while the counter write did not reach disk.</p>
 */
final class DeploymentStatusSequence {

    static final String SEQUENCE_COUNTER_TOPIC = "DeploymentStatusSequence";
    private static final Logger logger = LogManager.getLogger(DeploymentStatusSequence.class);

    private final Topic counterTopic;
    private final Topics runtimeStore;
    private final Topics processedDeployments;
    /** Last allocated sequence; null until initialised from the persisted counter and records. */
    private Long lastAllocated;

    DeploymentStatusSequence(Topics runtimeStore, Topics processedDeployments) {
        this.counterTopic = runtimeStore.lookup(SEQUENCE_COUNTER_TOPIC);
        this.runtimeStore = runtimeStore;
        this.processedDeployments = processedDeployments;
    }

    /**
     * Allocate the next sequence number, persisting the advanced counter before returning.
     *
     * @return a sequence number greater than any number handed out before
     * @throws IllegalStateException if the sequence is exhausted
     */
    synchronized long next() {
        if (lastAllocated == null) {
            lastAllocated = initialise();
        }
        if (lastAllocated == Long.MAX_VALUE) {
            throw new IllegalStateException("Deployment status sequence is exhausted");
        }
        long next = lastAllocated + 1;
        // Advance memory only after the write is queued; a failed persistence is recovered by the record scan above.
        lastAllocated = next;
        persist(next);
        return next;
    }

    /**
     * Highest sequence number in use: the persisted counter, or any record that got written before the counter
     * reached disk.
     */
    private long initialise() {
        long fromCounter = Coerce.toLong(counterTopic.getOnce());
        long fromRecords = highestPersistedSequence();
        long initialised = Math.max(Math.max(fromCounter, fromRecords), 0L);
        logger.atDebug().kv("sequence", initialised).log("Initialized deployment status sequence counter");
        return initialised;
    }

    private long highestPersistedSequence() {
        long highest = 0;
        for (Node node : processedDeployments) {
            if (!(node instanceof Topics)) {
                continue;
            }
            Object sequence = ((Topics) node).find(DeploymentStatusReplayPlan.SEQUENCE_KEY_NAME);
            highest = Math.max(highest, Coerce.toLong(sequence));
        }
        return highest;
    }

    /**
     * Write the counter and wait until the config publish queue has processed the write. This is what makes the
     * ordering "counter first, record second" hold on disk rather than only in memory.
     *
     * <p>{@code overrideValue} is used instead of {@code withValue} on purpose: the latter refuses a write whose
     * timestamp is older than the current modification time, and this counter must keep moving forward even when the
     * device clock was moved backwards.</p>
     */
    private void persist(long value) {
        Throwable error = runtimeStore.getContext().runOnPublishQueueAndWait(() -> counterTopic.overrideValue(value));
        if (error != null) {
            // The record still carries the sequence number, so a restart recovers it by scanning the records.
            logger.atError().kv("sequence", value).setCause(error)
                    .log("Unable to persist deployment status sequence counter");
            return;
        }
        // The value change queues its own transaction-log write; wait behind it so the counter is on disk before the
        // caller writes the record that uses the number.
        runtimeStore.getContext().runOnPublishQueueAndWait(() -> {
        });
    }
}
