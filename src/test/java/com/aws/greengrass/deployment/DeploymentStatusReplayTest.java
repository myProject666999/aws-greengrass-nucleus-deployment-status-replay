/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.deployment;

import com.aws.greengrass.config.Configuration;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.deployment.DeploymentStatusReplayPlan.PersistedStatusRecord;
import com.aws.greengrass.deployment.DeploymentStatusReplayPlan.Plan;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.iot.iotjobs.model.JobStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.aws.greengrass.deployment.DeploymentStatusKeeper.CONFIGURATION_ARN_KEY_NAME;
import static com.aws.greengrass.deployment.DeploymentStatusKeeper.DEPLOYMENT_ID_KEY_NAME;
import static com.aws.greengrass.deployment.DeploymentStatusKeeper.DEPLOYMENT_STATUS_DETAILS_KEY_NAME;
import static com.aws.greengrass.deployment.DeploymentStatusKeeper.DEPLOYMENT_STATUS_KEY_NAME;
import static com.aws.greengrass.deployment.DeploymentStatusKeeper.DEPLOYMENT_TYPE_KEY_NAME;
import static com.aws.greengrass.deployment.model.Deployment.DeploymentType.IOT_JOBS;
import static com.aws.greengrass.deployment.model.Deployment.DeploymentType.SHADOW;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests the replay plan and the replay loop without MQTT: in-memory config, fake subscribers and an injected sequence
 * source stand in for the cloud.
 */
@ExtendWith({MockitoExtension.class})
class DeploymentStatusReplayTest {

    private static final String SERVICE = "fakeSubscriber";
    private static final String JOB_1 = "job-1";
    private static final String JOB_2 = "job-2";
    private static final String ARN_1 = "arn:1";
    private static final String ARN_2 = "arn:2";
    private static final long BASE_LEGACY_TIMESTAMP = 1_700_000_000_000L;

    @Mock
    private DeploymentService deploymentService;

    private Context context;
    private DeploymentStatusKeeper keeper;
    private Topics processedDeployments;
    private final List<Map<String, Object>> delivered = new CopyOnWriteArrayList<>();
    private int legacyKeyOffset;
    private final AtomicInteger sequence = new AtomicInteger();

    @BeforeEach
    void setup() {
        context = new Context();
        Configuration config = new Configuration(context);
        when(deploymentService.getRuntimeConfig()).thenReturn(
                config.lookupTopics(GreengrassService.SERVICES_NAMESPACE_TOPIC,
                        DeploymentService.DEPLOYMENT_SERVICE_TOPICS, GreengrassService.RUNTIME_STORE_NAMESPACE_TOPIC));
        keeper = new DeploymentStatusKeeper();
        keeper.setDeploymentService(deploymentService);
        keeper.setSequenceSource(sequence::incrementAndGet);
        processedDeployments = keeper.getProcessedDeployments();
    }

    @AfterEach
    void afterEach() throws IOException {
        context.close();
    }

    // --- plan-level tests -----------------------------------------------------------------------------------------

    @Test
    void GIVEN_two_records_of_one_deployment_WHEN_computing_plan_THEN_only_the_terminal_one_replays() {
        List<PersistedStatusRecord> records = Arrays.asList(
                record("Sequence-2", 2L, job(JOB_1, JobStatus.SUCCEEDED)),
                record("Sequence-1", 1L, job(JOB_1, JobStatus.IN_PROGRESS)));

        Plan plan = DeploymentStatusReplayPlan.compute(IOT_JOBS, records);

        // Only the terminal status of the deployment is replayed; the earlier IN_PROGRESS is superseded.
        assertEquals(Arrays.asList("Sequence-2"), keys(plan.getToPublish()));
        assertEquals(Arrays.asList("Sequence-1"), keys(plan.getSuperseded()));
    }

    @Test
    void GIVEN_clock_moved_backwards_WHEN_computing_plan_THEN_sequence_not_clock_defines_order() {
        // A wall-clock ordering would put the record stamped 1000 before the one stamped 5000; the sequence says the
        // second happened after the first.
        List<PersistedStatusRecord> records = Arrays.asList(
                new PersistedStatusRecord("Sequence-1", 1L, 5000L, job(JOB_1, JobStatus.IN_PROGRESS)),
                new PersistedStatusRecord("Sequence-2", 2L, 1000L, job(JOB_2, JobStatus.SUCCEEDED)));

        Plan plan = DeploymentStatusReplayPlan.compute(IOT_JOBS, records);

        assertEquals(Arrays.asList("Sequence-1", "Sequence-2"), keys(plan.getToPublish()));
    }

    @Test
    void GIVEN_in_progress_then_succeeded_WHEN_computing_plan_THEN_only_succeeded_replays() {
        Plan plan = DeploymentStatusReplayPlan.compute(IOT_JOBS, Arrays.asList(
                record("Sequence-1", 1L, job(JOB_1, JobStatus.IN_PROGRESS)),
                record("Sequence-2", 2L, job(JOB_1, JobStatus.SUCCEEDED))));

        assertEquals(Arrays.asList("Sequence-2"), keys(plan.getToPublish()));
        assertEquals(JobStatus.SUCCEEDED.toString(), plan.getToPublish().get(0).getStatus());
    }

    @Test
    void GIVEN_interleaved_deployments_WHEN_computing_plan_THEN_relative_order_is_preserved() {
        Plan plan = DeploymentStatusReplayPlan.compute(IOT_JOBS, Arrays.asList(
                record("Sequence-1", 1L, job(JOB_1, JobStatus.IN_PROGRESS)),
                record("Sequence-2", 2L, job(JOB_2, JobStatus.IN_PROGRESS)),
                record("Sequence-3", 3L, job(JOB_1, JobStatus.SUCCEEDED)),
                record("Sequence-4", 4L, job(JOB_2, JobStatus.FAILED))));

        assertEquals(Arrays.asList("Sequence-3", "Sequence-4"), keys(plan.getToPublish()));
    }

    @Test
    void GIVEN_terminal_status_then_non_terminal_WHEN_computing_plan_THEN_terminal_wins_with_a_warning() {
        Plan plan = DeploymentStatusReplayPlan.compute(IOT_JOBS, Arrays.asList(
                record("Sequence-1", 1L, job(JOB_1, JobStatus.SUCCEEDED)),
                record("Sequence-2", 2L, job(JOB_1, JobStatus.IN_PROGRESS))));

        assertEquals(Arrays.asList("Sequence-1"), keys(plan.getToPublish()));
        assertEquals(1, plan.getTerminalOverrides().size());
    }

    @Test
    void GIVEN_no_terminal_status_WHEN_computing_plan_THEN_last_non_terminal_is_kept() {
        Plan plan = DeploymentStatusReplayPlan.compute(IOT_JOBS, Arrays.asList(
                record("Sequence-1", 1L, job(JOB_1, JobStatus.QUEUED)),
                record("Sequence-2", 2L, job(JOB_1, JobStatus.IN_PROGRESS))));

        assertEquals(Arrays.asList("Sequence-2"), keys(plan.getToPublish()));
        assertTrue(plan.getTerminalOverrides().isEmpty());
    }

    @Test
    void GIVEN_unknown_status_WHEN_computing_plan_THEN_record_is_quarantined() {
        Plan plan = DeploymentStatusReplayPlan.compute(IOT_JOBS, Arrays.asList(
                record("Sequence-1", 1L, job(JOB_1, "NOT_A_STATUS")),
                record("Sequence-2", 2L, job(JOB_1, JobStatus.SUCCEEDED))));

        assertEquals(Arrays.asList("Sequence-1"), keys(plan.getInvalid()));
        assertEquals(Arrays.asList("Sequence-2"), keys(plan.getToPublish()));
    }

    @Test
    void GIVEN_legacy_and_sequenced_records_WHEN_computing_plan_THEN_legacy_records_replay_first() {
        PersistedStatusRecord legacy = record("1700000000000", null, job(JOB_1, JobStatus.IN_PROGRESS));
        PersistedStatusRecord sequenced = record("Sequence-1", 1L, job(JOB_2, JobStatus.SUCCEEDED));

        Plan plan = DeploymentStatusReplayPlan.compute(IOT_JOBS, Arrays.asList(sequenced, legacy));

        assertEquals(Arrays.asList("1700000000000", "Sequence-1"), keys(plan.getToPublish()));
    }

    @Test
    void GIVEN_two_legacy_records_with_equal_timestamps_WHEN_sorting_THEN_key_breaks_the_tie() {
        PersistedStatusRecord a = new PersistedStatusRecord("1700000000000", null, 5L,
                job(JOB_1, JobStatus.IN_PROGRESS));
        PersistedStatusRecord b = new PersistedStatusRecord("1700000000001", null, 5L,
                job(JOB_2, JobStatus.IN_PROGRESS));

        List<PersistedStatusRecord> sorted = new ArrayList<>(Arrays.asList(b, a));
        sorted.sort(DeploymentStatusReplayPlan.REPLAY_ORDER);

        assertEquals(Arrays.asList("1700000000000", "1700000000001"), keys(sorted));
    }

    @Test
    void GIVEN_shadow_records_WHEN_computing_plan_THEN_merge_key_is_configuration_arn() {
        Map<String, Object> first = shadow(ARN_1, JobStatus.IN_PROGRESS);
        Map<String, Object> second = shadow(ARN_1, JobStatus.SUCCEEDED);
        Map<String, Object> other = shadow(ARN_2, JobStatus.IN_PROGRESS);

        Plan plan = DeploymentStatusReplayPlan.compute(SHADOW, Arrays.asList(
                record("Sequence-1", 1L, first),
                record("Sequence-2", 2L, second),
                record("Sequence-3", 3L, other)));

        assertEquals(Arrays.asList("Sequence-2", "Sequence-3"), keys(plan.getToPublish()));
    }

    // --- replay loop tests ----------------------------------------------------------------------------------------

    @Test
    void GIVEN_consumer_returns_false_WHEN_replaying_THEN_later_records_are_held_back() {
        persistLegacy(job(JOB_1, JobStatus.SUCCEEDED));
        persistLegacy(job(JOB_2, JobStatus.SUCCEEDED));
        keeper.registerDeploymentStatusConsumer(IOT_JOBS, this::recordButReject, SERVICE);

        keeper.publishPersistedStatusUpdates(IOT_JOBS);

        assertEquals(1, delivered.size());
        assertEquals(2, processedDeployments.children.size());
    }

    @Test
    void GIVEN_consumer_throws_WHEN_replaying_THEN_record_is_kept_and_not_lost() {
        Map<String, Object> details = job(JOB_1, JobStatus.SUCCEEDED);
        persistLegacy(details);
        keeper.registerDeploymentStatusConsumer(IOT_JOBS, record -> {
            throw new IllegalStateException("boom");
        }, SERVICE);

        keeper.publishPersistedStatusUpdates(IOT_JOBS);

        assertEquals(1, processedDeployments.children.size());
    }

    @Test
    void GIVEN_unknown_status_WHEN_replaying_THEN_it_is_quarantined_and_no_longer_blocks_the_queue() {
        PersistedStatusRecord bad = record("Sequence-1", 1L, job(JOB_1, "NOT_A_STATUS"));
        storeRecord(bad);
        storeRecord(record("Sequence-2", 2L, job(JOB_2, JobStatus.SUCCEEDED)));
        keeper.registerDeploymentStatusConsumer(IOT_JOBS, this::recordAndAccept, SERVICE);

        keeper.publishPersistedStatusUpdates(IOT_JOBS);

        assertEquals(Arrays.asList(JOB_2), deliveredDeploymentIds());
        assertEquals(0, processedDeployments.children.size());
        assertEquals(1, keeper.getQuarantine().contents().size());
    }

    @Test
    void GIVEN_more_than_the_limit_WHEN_quarantining_THEN_the_oldest_records_are_dropped() {
        for (int i = 1; i <= DeploymentStatusQuarantine.MAX_QUARANTINED_RECORDS + 5; i++) {
            storeRecord(record("Sequence-" + i, (long) i, job(JOB_1 + i, "NOT_A_STATUS")));
        }

        keeper.publishPersistedStatusUpdates(IOT_JOBS);

        List<Map<String, Object>> quarantined = keeper.getQuarantine().contents();
        assertEquals(DeploymentStatusQuarantine.MAX_QUARANTINED_RECORDS, quarantined.size());
        assertEquals(6L, ((Number) quarantined.get(0)
                .get(DeploymentStatusReplayPlan.SEQUENCE_KEY_NAME)).longValue());
    }

    @Test
    void GIVEN_two_threads_replay_at_once_WHEN_they_run_THEN_each_record_is_delivered_once() throws Exception {
        for (int i = 1; i <= 20; i++) {
            storeRecord(record("Sequence-" + i, (long) i, job("job-" + i, JobStatus.SUCCEEDED)));
        }
        // Block the first delivery until the second thread has entered the replay, to force interleaving.
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        keeper.registerDeploymentStatusConsumer(IOT_JOBS, details -> {
            if (calls.incrementAndGet() == 1) {
                firstEntered.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            delivered.add(details);
            return true;
        }, SERVICE);

        Thread first = new Thread(() -> keeper.publishPersistedStatusUpdates(IOT_JOBS));
        Thread second = new Thread(() -> keeper.publishPersistedStatusUpdates(IOT_JOBS));
        first.start();
        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
        second.start();
        release.countDown();
        first.join(10_000);
        second.join(10_000);

        assertEquals(20, delivered.size());
        assertEquals(20, deliveredDeploymentIds().stream().distinct().count());
        assertEquals(0, processedDeployments.children.size());
    }

    @Test
    void WHEN_persisting_twice_in_the_same_millisecond_THEN_both_records_are_kept() {
        keeper.registerDeploymentStatusConsumer(IOT_JOBS, this::recordButReject, SERVICE);
        // Use the real persisted counter rather than the injected one. Two statuses recorded in the same millisecond
        // would have collided on a timestamp key; the sequence keeps them apart.
        keeper.setSequenceSource(null);

        keeper.persistAndPublishDeploymentStatus(JOB_1, "gg-1", ARN_1, IOT_JOBS, JobStatus.IN_PROGRESS.toString(),
                new HashMap<>(), new ArrayList<>());
        keeper.persistAndPublishDeploymentStatus(JOB_2, "gg-2", ARN_2, IOT_JOBS, JobStatus.SUCCEEDED.toString(),
                new HashMap<>(), new ArrayList<>());

        assertEquals(2, processedDeployments.children.size());
        assertEquals(Arrays.asList("Sequence-1", "Sequence-2"),
                processedDeployments.children.values().stream().map(com.aws.greengrass.config.Node::getName)
                        .sorted().collect(Collectors.toList()));
    }

    @Test
    void GIVEN_no_subscriber_WHEN_replaying_THEN_records_are_kept_for_a_later_replay() {
        storeRecord(record("Sequence-1", 1L, job(JOB_1, JobStatus.SUCCEEDED)));

        keeper.publishPersistedStatusUpdates(IOT_JOBS);

        assertEquals(1, processedDeployments.children.size());
    }

    @Test
    void WHEN_callback_payload_is_built_THEN_internal_sequence_field_is_not_exposed() {
        List<Map<String, Object>> payloads = new ArrayList<>();
        keeper.registerDeploymentStatusConsumer(IOT_JOBS, details -> {
            payloads.add(details);
            return true;
        }, SERVICE);
        storeRecord(record("Sequence-1", 1L, job(JOB_1, JobStatus.SUCCEEDED)));

        keeper.publishPersistedStatusUpdates(IOT_JOBS);

        assertEquals(1, payloads.size());
        assertFalse(payloads.get(0).containsKey(DeploymentStatusReplayPlan.SEQUENCE_KEY_NAME));
        assertEquals(JOB_1, payloads.get(0).get(DEPLOYMENT_ID_KEY_NAME));
    }

    @Test
    void GIVEN_callback_running_WHEN_it_observes_config_THEN_record_is_still_persisted() {
        List<Integer> pendingDuringCallback = new ArrayList<>();
        keeper.registerDeploymentStatusConsumer(IOT_JOBS, details -> {
            pendingDuringCallback.add(processedDeployments.children.size());
            return true;
        }, SERVICE);
        storeRecord(record("Sequence-1", 1L, job(JOB_1, JobStatus.SUCCEEDED)));

        keeper.publishPersistedStatusUpdates(IOT_JOBS);

        assertEquals(Arrays.asList(1), pendingDuringCallback);
        assertEquals(0, processedDeployments.children.size());
    }

    @Test
    void GIVEN_legacy_and_new_records_WHEN_replaying_THEN_legacy_is_delivered_first() {
        keeper.registerDeploymentStatusConsumer(IOT_JOBS, this::recordAndAccept, SERVICE);
        // New-format record persisted first, then a legacy record written by an older version.
        storeRecord(record("Sequence-1", 1L, job(JOB_2, JobStatus.SUCCEEDED)));
        storeRecord(record("1700000000000", null, job(JOB_1, JobStatus.IN_PROGRESS)));

        keeper.publishPersistedStatusUpdates(IOT_JOBS);

        assertEquals(Arrays.asList(JOB_1, JOB_2), deliveredDeploymentIds());
        assertEquals(0, processedDeployments.children.size());
    }

    @Test
    void GIVEN_records_already_persisted_WHEN_counter_is_initialised_THEN_it_starts_above_them() {
        storeRecord(record("Sequence-7", 7L, job(JOB_1, JobStatus.SUCCEEDED)));
        storeRecord(record("Sequence-42", 42L, job(JOB_2, JobStatus.SUCCEEDED)));

        // A fresh keeper over the same config recovers the highest sequence instead of restarting at 1.
        DeploymentStatusKeeper restarted = new DeploymentStatusKeeper();
        restarted.setDeploymentService(deploymentService);
        long next = new DeploymentStatusSequence(restarted.getProcessedDeployments().parent,
                restarted.getProcessedDeployments()).next();

        assertTrue(next > 42, "next sequence " + next + " must exceed every persisted record");
    }

    @Test
    void GIVEN_record_with_unknown_deployment_type_WHEN_replaying_THEN_it_is_quarantined_and_stops_blocking() {
        keeper.registerDeploymentStatusConsumer(IOT_JOBS, this::recordAndAccept, SERVICE);
        Map<String, Object> unknownType = job(JOB_1, JobStatus.SUCCEEDED);
        unknownType.put(DEPLOYMENT_TYPE_KEY_NAME, "NOT_A_TYPE");
        storeRecord(record("Sequence-1", 1L, unknownType));
        storeRecord(record("Sequence-2", 2L, job(JOB_2, JobStatus.SUCCEEDED)));

        keeper.publishPersistedStatusUpdates(IOT_JOBS);

        assertEquals(Arrays.asList(JOB_2), deliveredDeploymentIds());
        assertEquals(0, processedDeployments.children.size());
        assertEquals(1, keeper.getQuarantine().contents().size());
    }

    @Test
    void GIVEN_the_same_record_WHEN_the_comparator_is_used_THEN_it_returns_zero() {
        PersistedStatusRecord a = new PersistedStatusRecord("Sequence-1", 1L, 0L, job(JOB_1, JobStatus.IN_PROGRESS));
        PersistedStatusRecord b = new PersistedStatusRecord("Sequence-1", 1L, 0L, job(JOB_1, JobStatus.IN_PROGRESS));
        PersistedStatusRecord legacyA = new PersistedStatusRecord("1700000000000", null, 1700L,
                job(JOB_1, JobStatus.IN_PROGRESS));
        PersistedStatusRecord legacyB = new PersistedStatusRecord("1700000000000", null, 1700L,
                job(JOB_1, JobStatus.IN_PROGRESS));

        assertEquals(0, DeploymentStatusReplayPlan.REPLAY_ORDER.compare(a, b));
        assertEquals(0, DeploymentStatusReplayPlan.REPLAY_ORDER.compare(legacyA, legacyB));
    }

    @Test
    void GIVEN_legacy_record_with_unparseable_key_WHEN_replaying_THEN_it_does_not_crash_and_replays_first() {
        keeper.registerDeploymentStatusConsumer(IOT_JOBS, this::recordAndAccept, SERVICE);
        storeRecord(record("some-old-key", null, job(JOB_1, JobStatus.IN_PROGRESS)));
        storeRecord(record("Sequence-1", 1L, job(JOB_2, JobStatus.SUCCEEDED)));

        keeper.publishPersistedStatusUpdates(IOT_JOBS);

        assertEquals(Arrays.asList(JOB_1, JOB_2), deliveredDeploymentIds());
    }

    // --- helpers --------------------------------------------------------------------------------------------------

    private Boolean recordAndAccept(Map<String, Object> details) {
        delivered.add(details);
        return true;
    }

    private Boolean recordButReject(Map<String, Object> details) {
        delivered.add(details);
        return false;
    }

    private void persistLegacy(Map<String, Object> details) {
        // Distinct, increasing legacy keys that deliberately ignore the (possibly duplicated) clock reading.
        storeRecord(record(String.valueOf(BASE_LEGACY_TIMESTAMP + legacyKeyOffset++), null, details));
    }

    private void storeRecord(PersistedStatusRecord record) {
        Map<String, Object> stored = new HashMap<>(record.getFields());
        if (record.getSequence() != null) {
            stored.put(DeploymentStatusReplayPlan.SEQUENCE_KEY_NAME, record.getSequence());
        }
        processedDeployments.createInteriorChild(record.getKey()).replaceAndWait(stored);
    }

    private static PersistedStatusRecord record(String key, Long sequence, Map<String, Object> fields) {
        long legacyTimestamp;
        try {
            legacyTimestamp = Long.parseLong(key);
        } catch (NumberFormatException e) {
            legacyTimestamp = 0L;
        }
        return new PersistedStatusRecord(key, sequence, legacyTimestamp, fields);
    }

    private static Map<String, Object> job(String deploymentId, Object status) {
        Map<String, Object> details = new HashMap<>();
        details.put(DEPLOYMENT_ID_KEY_NAME, deploymentId);
        details.put(CONFIGURATION_ARN_KEY_NAME, ARN_1);
        details.put(DEPLOYMENT_TYPE_KEY_NAME, IOT_JOBS.toString());
        details.put(DEPLOYMENT_STATUS_KEY_NAME, status.toString());
        details.put(DEPLOYMENT_STATUS_DETAILS_KEY_NAME, new HashMap<>());
        return details;
    }

    private static Map<String, Object> shadow(String arn, Object status) {
        Map<String, Object> details = job(arn, status);
        details.put(DEPLOYMENT_TYPE_KEY_NAME, SHADOW.toString());
        details.put(CONFIGURATION_ARN_KEY_NAME, arn);
        return details;
    }

    private static List<String> keys(List<PersistedStatusRecord> records) {
        return records.stream().map(PersistedStatusRecord::getKey).collect(Collectors.toList());
    }

    private List<String> deliveredDeploymentIds() {
        return delivered.stream().map(map -> String.valueOf(map.get(DEPLOYMENT_ID_KEY_NAME)))
                .collect(Collectors.toList());
    }
}

