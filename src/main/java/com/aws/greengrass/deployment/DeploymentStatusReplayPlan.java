/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.deployment;

import com.aws.greengrass.deployment.model.Deployment.DeploymentType;
import com.aws.greengrass.util.Coerce;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import software.amazon.awssdk.iot.iotjobs.model.JobStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.aws.greengrass.deployment.DeploymentStatusKeeper.CONFIGURATION_ARN_KEY_NAME;
import static com.aws.greengrass.deployment.DeploymentStatusKeeper.DEPLOYMENT_ID_KEY_NAME;
import static com.aws.greengrass.deployment.DeploymentStatusKeeper.DEPLOYMENT_STATUS_KEY_NAME;

/**
 * Pure logic deciding which persisted deployment status records must be replayed to the cloud, in which order, and
 * which records can be dropped or quarantined. No MQTT, config or clock access happens here so the replay semantics
 * can be tested in isolation.
 *
 * <p>Records are ordered by the sequence number assigned when they were persisted. Records written by versions that
 * predate the sequence number (keyed by a millisecond timestamp, without a {@code Sequence} field) are ordered before
 * all sequenced records, by their timestamp and then lexicographically by their config key. The comparator is a total
 * order and never returns a non-zero value for records that compare equal.</p>
 *
 * <p>For a single deployment only the status the cloud cares about is kept: the last terminal status. Earlier
 * non-terminal statuses (for example IN_PROGRESS) are dropped, and so is anything recorded after a terminal status. If
 * no terminal status exists yet, the last non-terminal status is kept. Records of different deployments never change
 * their relative order because of merging: survivors keep their original positions.</p>
 */
final class DeploymentStatusReplayPlan {

    /** Config field holding the sequence number of a record persisted by this version. */
    static final String SEQUENCE_KEY_NAME = "Sequence";

    private static final Set<String> KNOWN_STATUSES = knownStatuses();
    // Statuses that end the lifecycle of a job execution. QUEUED and IN_PROGRESS are the only non-terminal
    // statuses that can be persisted.
    private static final Set<String> TERMINAL_STATUSES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            JobStatus.SUCCEEDED.toString(), JobStatus.FAILED.toString(), JobStatus.REJECTED.toString(),
            JobStatus.CANCELED.toString(), JobStatus.TIMED_OUT.toString(), JobStatus.REMOVED.toString())));

    /**
     * Order in which persisted records must be replayed: legacy records first (by timestamp, then key), then
     * sequenced records by sequence number.
     */
    static final Comparator<PersistedStatusRecord> REPLAY_ORDER = (o1, o2) -> {
        if (o1.getSequence() == null && o2.getSequence() != null) {
            return -1;
        }
        if (o1.getSequence() != null && o2.getSequence() == null) {
            return 1;
        }
        if (o1.getSequence() == null) {
            int byTimestamp = Long.compare(o1.getLegacyTimestamp(), o2.getLegacyTimestamp());
            if (byTimestamp != 0) {
                return byTimestamp;
            }
            return o1.getKey().compareTo(o2.getKey());
        }
        return Long.compare(o1.getSequence(), o2.getSequence());
    };

    private DeploymentStatusReplayPlan() {
    }

    /**
     * Build a replay plan from the persisted records of a single deployment type.
     *
     * @param type    deployment type the records belong to; decides how the deployment identity is derived
     * @param records persisted status records, in any order
     * @return plan describing what to replay, what to drop and what to quarantine
     */
    static Plan compute(DeploymentType type, Collection<PersistedStatusRecord> records) {
        List<PersistedStatusRecord> ordered = new ArrayList<>(records);
        ordered.sort(REPLAY_ORDER);

        List<PersistedStatusRecord> invalidRecords = new ArrayList<>();
        Set<PersistedStatusRecord> invalid = new HashSet<>();
        Map<String, List<PersistedStatusRecord>> byDeployment = new LinkedHashMap<>();
        for (PersistedStatusRecord record : ordered) {
            String deploymentKey = deploymentKey(type, record.getFields());
            if (!KNOWN_STATUSES.contains(record.getStatus()) || deploymentKey == null || deploymentKey.isEmpty()) {
                invalid.add(record);
                invalidRecords.add(record);
                continue;
            }
            byDeployment.computeIfAbsent(deploymentKey, key -> new ArrayList<>()).add(record);
        }

        Set<PersistedStatusRecord> toReplay = new HashSet<>();
        List<TerminalOverride> terminalOverrides = new ArrayList<>();
        for (List<PersistedStatusRecord> deploymentRecords : byDeployment.values()) {
            PersistedStatusRecord kept = selectStatusToReplay(deploymentRecords);
            toReplay.add(kept);
            PersistedStatusRecord lastRecord = deploymentRecords.get(deploymentRecords.size() - 1);
            if (!kept.getKey().equals(lastRecord.getKey())) {
                // A terminal status was followed by a non-terminal one. The terminal status wins, but this should not
                // happen in practice, so surface it to the caller for a warning.
                terminalOverrides.add(new TerminalOverride(kept, lastRecord));
            }
        }

        List<PersistedStatusRecord> toPublish = new ArrayList<>();
        List<PersistedStatusRecord> superseded = new ArrayList<>();
        for (PersistedStatusRecord record : ordered) {
            if (invalid.contains(record)) {
                continue;
            }
            if (toReplay.contains(record)) {
                toPublish.add(record);
            } else {
                superseded.add(record);
            }
        }
        return new Plan(toPublish, superseded, invalidRecords, terminalOverrides);
    }

    private static Set<String> knownStatuses() {
        Set<String> known = new HashSet<>();
        for (JobStatus status : JobStatus.values()) {
            if (status != JobStatus.UNKNOWN_ENUM_VALUE) {
                known.add(status.toString());
            }
        }
        return Collections.unmodifiableSet(known);
    }

    /**
     * Deployment identity used to merge records. IoT Jobs deployments are identified by the job id, shadow
     * deployments by the configuration ARN.
     */
    private static String deploymentKey(DeploymentType type, Map<String, Object> fields) {
        if (DeploymentType.SHADOW == type) {
            String configurationArn = Coerce.toString(fields.get(CONFIGURATION_ARN_KEY_NAME));
            if (configurationArn != null && !configurationArn.isEmpty()) {
                return configurationArn;
            }
        }
        return Coerce.toString(fields.get(DEPLOYMENT_ID_KEY_NAME));
    }

    /**
     * Select the single record that matters for one deployment: the last terminal status, or - if the deployment never
     * reached a terminal status - the last non-terminal status.
     */
    private static PersistedStatusRecord selectStatusToReplay(List<PersistedStatusRecord> deploymentRecords) {
        for (int i = deploymentRecords.size() - 1; i >= 0; i--) {
            if (TERMINAL_STATUSES.contains(deploymentRecords.get(i).getStatus())) {
                return deploymentRecords.get(i);
            }
        }
        return deploymentRecords.get(deploymentRecords.size() - 1);
    }

    /**
     * One persisted deployment status record as read back from the config store.
     */
    @Getter
    static final class PersistedStatusRecord {
        /**
         * Config key the record is persisted under: {@code Sequence-<n>}, or a millisecond timestamp for legacy
         * records.
         */
        private final String key;
        /** Sequence number assigned when the record was persisted; null for records from older versions. */
        private final Long sequence;
        /** Timestamp encoded in the config key of a legacy record, 0 when the key cannot be parsed. */
        private final long legacyTimestamp;
        private final Map<String, Object> fields;
        private final String status;

        PersistedStatusRecord(String key, Long sequence, long legacyTimestamp, Map<String, Object> fields) {
            this.key = key;
            this.sequence = sequence;
            this.legacyTimestamp = legacyTimestamp;
            this.fields = fields;
            this.status = Coerce.toString(fields.get(DEPLOYMENT_STATUS_KEY_NAME));
        }

        /**
         * Build the map handed to status subscribers. Bookkeeping fields are not part of the callback contract, so
         * they are hidden from subscribers.
         *
         * @return fresh map of the persisted deployment status
         */
        Map<String, Object> toCallbackPayload() {
            Map<String, Object> payload = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            payload.putAll(fields);
            payload.remove(SEQUENCE_KEY_NAME);
            return payload;
        }
    }

    /**
     * Outcome of planning a replay round.
     */
    @Getter
    @AllArgsConstructor(access = AccessLevel.PACKAGE)
    static final class Plan {
        /** Records to hand to subscribers, in replay order. */
        private final List<PersistedStatusRecord> toPublish;
        /** Records made obsolete by a newer status of the same deployment; remove without publishing. */
        private final List<PersistedStatusRecord> superseded;
        /** Records that can never be replayed; move to quarantine so they stop blocking other records. */
        private final List<PersistedStatusRecord> invalid;
        /** Deployments whose terminal status is followed by a non-terminal one. */
        private final List<TerminalOverride> terminalOverrides;
    }

    /**
     * A non-terminal record recorded after the terminal status of the same deployment.
     */
    @Getter
    @AllArgsConstructor(access = AccessLevel.PACKAGE)
    static final class TerminalOverride {
        private final PersistedStatusRecord keptRecord;
        private final PersistedStatusRecord overriddenRecord;
    }
}
