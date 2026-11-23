/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.deployment;

import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.aws.greengrass.deployment.DeploymentStatusKeeper.DEPLOYMENT_TYPE_KEY_NAME;
import static com.aws.greengrass.deployment.DeploymentStatusReplayPlan.PersistedStatusRecord;

/**
 * Stores deployment status records that can never be reported to the cloud (for example an unknown status string or
 * a record that cannot be deserialized). They are moved out of the replay queue so they stop blocking later records,
 * but kept for diagnostics.
 *
 * <p>Quarantined records are bounded to keep the store from growing without limit: when the limit is exceeded the
 * oldest quarantined record is discarded. Every record keeps the sequence number it had when it was persisted, so
 * ordering and pruning survive a restart.</p>
 */
class DeploymentStatusQuarantine {

    static final String QUARANTINE_TOPIC = "QuarantinedDeploymentStatuses";
    /** Maximum number of quarantined records kept; the oldest is dropped beyond this. */
    static final int MAX_QUARANTINED_RECORDS = 100;
    private static final Long OLDEST = Long.MIN_VALUE;
    private static final Logger logger = LogManager.getLogger(DeploymentStatusQuarantine.class);

    private final Topics quarantineTopics;

    DeploymentStatusQuarantine(Topics quarantineTopics) {
        this.quarantineTopics = quarantineTopics;
    }

    /**
     * Move a record into quarantine, discarding the oldest quarantined record if the limit is exceeded.
     *
     * @param typeLabel deployment type of the record, or the unparseable value that stood in its place
     * @param record    record that cannot be reported
     */
    synchronized void add(String typeLabel, PersistedStatusRecord record) {
        Topics entry = quarantineTopics.createInteriorChild(record.getKey());
        Map<String, Object> stored = record.toCallbackPayload();
        if (record.getSequence() != null) {
            stored.put(DeploymentStatusReplayPlan.SEQUENCE_KEY_NAME, record.getSequence());
        }
        stored.put(DEPLOYMENT_TYPE_KEY_NAME, typeLabel);
        entry.replaceAndWait(stored);
        prune();
    }

    /**
     * Quarantined records, oldest first. Used by tests and diagnostics.
     *
     * @return list of quarantined records
     */
    synchronized List<Map<String, Object>> contents() {
        return quarantineTopics.children.values().stream()
                .filter(node -> node instanceof Topics)
                .sorted(Comparator.comparingLong(node -> sortKey((Topics) node)))
                .map(node -> ((Topics) node).toPOJO())
                .collect(Collectors.toList());
    }

    private static long sortKey(Topics entry) {
        Topic sequence = entry.findLeafChild(DeploymentStatusReplayPlan.SEQUENCE_KEY_NAME);
        if (sequence != null && sequence.getOnce() instanceof Number) {
            return ((Number) sequence.getOnce()).longValue();
        }
        return OLDEST;
    }

    private void prune() {
        List<Topics> entries = new ArrayList<>();
        quarantineTopics.forEach(node -> {
            if (node instanceof Topics) {
                entries.add((Topics) node);
            }
        });
        if (entries.size() <= MAX_QUARANTINED_RECORDS) {
            return;
        }
        entries.sort(Comparator.comparingLong(DeploymentStatusQuarantine::sortKey));
        int excess = entries.size() - MAX_QUARANTINED_RECORDS;
        for (int i = 0; i < excess; i++) {
            logger.atDebug().kv("statusKey", entries.get(i).getName())
                    .log("Discarding oldest quarantined deployment status");
            quarantineTopics.remove(entries.get(i));
        }
    }
}
