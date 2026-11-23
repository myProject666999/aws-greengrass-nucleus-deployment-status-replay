/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.deployment;

import com.aws.greengrass.config.Node;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.DefaultConcurrentHashMap;
import com.aws.greengrass.util.LockFactory;
import com.aws.greengrass.util.LockScope;
import lombok.AccessLevel;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static com.aws.greengrass.deployment.DeploymentStatusReplayPlan.PersistedStatusRecord;
import static com.aws.greengrass.deployment.DeploymentStatusReplayPlan.Plan;
import static com.aws.greengrass.deployment.DeploymentStatusReplayPlan.TerminalOverride;
import static com.aws.greengrass.deployment.model.Deployment.DeploymentType;

/**
 * Persists deployment status updates and replays the ones that could not be reported while the device was offline.
 *
 * <p>Each update is stored under a key derived from a monotonically increasing sequence number (see
 * {@link DeploymentStatusSequence}); updates written by older versions are keyed by a millisecond timestamp and are
 * ordered before sequenced updates. On replay the records of a deployment type are merged so only the status the
 * cloud cares about is sent (see {@link DeploymentStatusReplayPlan}), obsolete records are dropped, records that can
 * never be sent are quarantined, and network failures stop the round while keeping the remaining records in order.</p>
 */
public class DeploymentStatusKeeper {

    public static final String PROCESSED_DEPLOYMENTS_TOPICS = "ProcessedDeployments";
    // the field contains job id for job deployment; config arn for shadow deployment
    public static final String DEPLOYMENT_ID_KEY_NAME = "DeploymentId";
    public static final String GG_DEPLOYMENT_ID_KEY_NAME = "GreengrassDeploymentId";
    public static final String CONFIGURATION_ARN_KEY_NAME = "ConfigurationArn";
    public static final String DEPLOYMENT_TYPE_KEY_NAME = "DeploymentType";
    public static final String DEPLOYMENT_ROOT_PACKAGES_KEY_NAME = "DeploymentRootPackages";
    public static final String DEPLOYMENT_STATUS_KEY_NAME = "DeploymentStatus";
    public static final String DEPLOYMENT_STATUS_DETAILS_KEY_NAME = "DeploymentStatusDetails";
    /** Prefix of the config key of a status record persisted by this version, followed by its sequence number. */
    static final String SEQUENCE_KEY_PREFIX = "Sequence-";
    private static final String DEPLOYMENT_TYPE_LOG_KEY = "deploymentType";
    private static final String STATUS_KEY_LOG_KEY = "statusKey";
    private static final Logger logger = LogManager.getLogger(DeploymentStatusKeeper.class);
    private final Map<DeploymentType, Map<String, Function<Map<String, Object>, Boolean>>> deploymentStatusConsumerMap
            = new ConcurrentHashMap<>();
    private static final Map<DeploymentType, Lock> lockMap =
            new DefaultConcurrentHashMap<>(() -> LockFactory.newReentrantLock("deploymentStatusKeeper"));
    @Setter
    private DeploymentService deploymentService;
    private Topics processedDeployments;
    private DeploymentStatusSequence sequence;
    private DeploymentStatusQuarantine quarantine;
    // Guards the sweep for records whose deployment type cannot be parsed: it touches records of every type, so it
    // cannot rely on any single deployment type's lock.
    private static final Object UNKNOWN_TYPE_SWEEP_LOCK = new Object();
    // Overrides the persisted sequence allocator; only used by tests in this package.
    @Setter(AccessLevel.PACKAGE)
    private LongSupplier sequenceSource;

    /**
     * Register call backs for receiving deployment status updates for a particular deployment type .
     *
     * @param type        determines which deployment type the call back consumes
     * @param consumer    deployment status details
     * @param serviceName subscribing service name
     * @return true if call back is registered.
     */
    public boolean registerDeploymentStatusConsumer(DeploymentType type,
                                                    Function<Map<String, Object>, Boolean> consumer,
                                                    String serviceName) {
        Map<String, Function<Map<String, Object>, Boolean>> map = deploymentStatusConsumerMap
                .getOrDefault(type, new ConcurrentHashMap<>());
        map.putIfAbsent(serviceName, consumer);
        return deploymentStatusConsumerMap.put(type, map) == null;
    }

    /**
     * Persist deployment status in kernel config.
     *
     * @param deploymentId     id for the deployment - job id for jobs and config arn for shadow
     * @param ggDeploymentId   greengrass deployment id for the deployment from GG cloud
     * @param configurationArn arn for deployment target configuration.
     * @param deploymentType   type of deployment.
     * @param status           status of deployment.
     * @param statusDetails    other details of deployment status.
     * @param rootPackages     root packages in the deployment.
     * @throws IllegalArgumentException for invalid deployment type
     */
    @SuppressWarnings("PMD.UseObjectForClearerAPI")
    public void persistAndPublishDeploymentStatus(String deploymentId, String ggDeploymentId, String configurationArn,
                                                  DeploymentType deploymentType, String status,
                                                  Map<String, Object> statusDetails, List<String> rootPackages) {

        //While this method is being run, another thread could be running the publishPersistedStatusUpdates
        // method which consumes the data in config from the same topics. These two thread needs to be synchronized
        try (LockScope ls = LockScope.lock(lockMap.get(deploymentType))) {
            logger.atDebug().kv(GG_DEPLOYMENT_ID_KEY_NAME, ggDeploymentId).kv(DEPLOYMENT_ID_KEY_NAME, deploymentId)
                    .kv(DEPLOYMENT_STATUS_KEY_NAME, status).log("Storing deployment status");
            Map<String, Object> deploymentDetails = new HashMap<>();
            deploymentDetails.put(DEPLOYMENT_ID_KEY_NAME, deploymentId);
            deploymentDetails.put(GG_DEPLOYMENT_ID_KEY_NAME, ggDeploymentId);
            deploymentDetails.put(CONFIGURATION_ARN_KEY_NAME, configurationArn);
            deploymentDetails.put(DEPLOYMENT_TYPE_KEY_NAME, deploymentType.toString());
            deploymentDetails.put(DEPLOYMENT_STATUS_KEY_NAME, status);
            deploymentDetails.put(DEPLOYMENT_STATUS_DETAILS_KEY_NAME, statusDetails);
            deploymentDetails.put(DEPLOYMENT_ROOT_PACKAGES_KEY_NAME, rootPackages);

            // The sequence is allocated - and the counter persisted - before the record is written so that a crash
            // between the two only leaves a hole in the sequence, never a record the counter has not reached.
            long nextSequence = nextSequence();
            deploymentDetails.put(DeploymentStatusReplayPlan.SEQUENCE_KEY_NAME, nextSequence);
            Topics processedDeployments = getProcessedDeployments();
            Topics thisJob = processedDeployments.createInteriorChild(SEQUENCE_KEY_PREFIX + nextSequence);
            thisJob.replaceAndWait(deploymentDetails);
            logger.atInfo().kv(GG_DEPLOYMENT_ID_KEY_NAME, ggDeploymentId).kv(DEPLOYMENT_ID_KEY_NAME, deploymentId)
                    .kv(DEPLOYMENT_STATUS_KEY_NAME, status).log("Stored deployment status");
        }
        publishPersistedStatusUpdates(deploymentType);
    }

    /**
     * Invokes the call-backs with persisted deployment status updates for deployments with specified type.
     * This is called by IotJobsHelper/MqttJobsHelper when connection is re-established to update cloud of all
     * all deployments the device performed when offline
     *
     * @param type deployment type
     */
    public void publishPersistedStatusUpdates(DeploymentType type) {
        try (LockScope ls = LockScope.lock(lockMap.get(type))) {
            Topics processedDeployments = getProcessedDeployments();
            quarantineRecordsWithUnknownType(processedDeployments);
            Plan plan = DeploymentStatusReplayPlan.compute(type, readPersistedRecords(processedDeployments, type));
            warnAboutOverriddenTerminalStatuses(type, plan);

            // Obsolete and unprocessable records do not depend on the network, so drop them even if a later record
            // cannot be sent this round.
            for (PersistedStatusRecord record : plan.getSuperseded()) {
                logger.atDebug().kv(DEPLOYMENT_TYPE_LOG_KEY, type).kv(STATUS_KEY_LOG_KEY, record.getKey())
                        .kv(DEPLOYMENT_STATUS_KEY_NAME, record.getStatus())
                        .log("Discarding superseded deployment status");
                removeRecord(processedDeployments, record);
            }
            for (PersistedStatusRecord record : plan.getInvalid()) {
                logger.atWarn().kv(DEPLOYMENT_TYPE_LOG_KEY, type).kv(STATUS_KEY_LOG_KEY, record.getKey())
                        .kv(DEPLOYMENT_STATUS_KEY_NAME, record.getStatus())
                        .log("Quarantining deployment status that cannot be reported");
                getQuarantine().add(type.toString(), record);
                removeRecord(processedDeployments, record);
            }

            List<Function<Map<String, Object>, Boolean>> consumers = getConsumersForDeploymentType(type);
            logger.atDebug().kv(DEPLOYMENT_TYPE_LOG_KEY, type).kv("numberOfSubscribers", consumers.size())
                    .kv("numberOfUpdates", plan.getToPublish().size())
                    .log("Updating status of persisted deployments to subscribers");
            if (consumers.isEmpty() && !plan.getToPublish().isEmpty()) {
                // Nobody is listening yet (for example the cloud connection was never configured). Keep the records
                // so they are reported once a subscriber registers, rather than dropping them.
                logger.atDebug().kv(DEPLOYMENT_TYPE_LOG_KEY, type)
                        .log("No subscriber for persisted deployment status; keeping it for a later replay");
                return;
            }
            for (PersistedStatusRecord record : plan.getToPublish()) {
                DeliveryResult result = notifyConsumers(consumers, record);
                if (DeliveryResult.FAILED == result) {
                    // If one deployment update fails, exit the loop to ensure the update order.
                    logger.atDebug().log("Unable to update status of persisted deployments. Retry later");
                    break;
                }
                if (DeliveryResult.ABORTED == result) {
                    // A consumer threw an unchecked exception: the record may or may not have been delivered, so keep
                    // it untouched and stop to preserve the order.
                    logger.atWarn().kv(DEPLOYMENT_TYPE_LOG_KEY, type).kv(STATUS_KEY_LOG_KEY, record.getKey())
                            .log("Subscriber threw an exception while updating persisted deployment status");
                    break;
                }
                // The record has been delivered to every subscriber; only now is it safe to remove it.
                removeRecord(processedDeployments, record);
            }
        }
    }

    /** How far a record got when handed to the subscribers. */
    private enum DeliveryResult {
        DELIVERED,
        FAILED,
        ABORTED
    }

    /**
     * Notify subscribers of one record.
     *
     * @return {@link DeliveryResult#DELIVERED} when every subscriber accepted the update,
     *     {@link DeliveryResult#FAILED} when one asked to retry, and {@link DeliveryResult#ABORTED} when a subscriber
     *     threw an unchecked exception
     */
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private DeliveryResult notifyConsumers(List<Function<Map<String, Object>, Boolean>> consumers,
                                           PersistedStatusRecord record) {
        Map<String, Object> payload = record.toCallbackPayload();
        for (Function<Map<String, Object>, Boolean> consumer : consumers) {
            boolean accepted;
            try {
                accepted = consumer.apply(payload);
            } catch (RuntimeException e) {
                logger.atError().kv(STATUS_KEY_LOG_KEY, record.getKey())
                        .kv(DEPLOYMENT_STATUS_KEY_NAME, record.getStatus()).setCause(e)
                        .log("Subscriber failed to process persisted deployment status");
                return DeliveryResult.ABORTED;
            }
            if (!accepted) {
                return DeliveryResult.FAILED;
            }
        }
        return DeliveryResult.DELIVERED;
    }

    /**
     * Records whose deployment type cannot be parsed would never be seen by any type's replay and would stay in the
     * queue forever. Move them to quarantine, where they are bounded and visible.
     */
    private void quarantineRecordsWithUnknownType(Topics processedDeployments) {
        synchronized (UNKNOWN_TYPE_SWEEP_LOCK) {
            for (Node node : processedDeployments) {
                if (!(node instanceof Topics)) {
                    continue;
                }
                Topics deploymentDetails = (Topics) node;
                Object typeValue = deploymentDetails.find(DEPLOYMENT_TYPE_KEY_NAME);
                if (Coerce.toEnum(DeploymentType.class, typeValue) != null) {
                    continue;
                }
                PersistedStatusRecord record = new PersistedStatusRecord(deploymentDetails.getName(),
                        sequenceOf(deploymentDetails), legacyTimestamp(deploymentDetails.getName()),
                        deploymentDetails.toPOJO());
                logger.atWarn().kv(STATUS_KEY_LOG_KEY, record.getKey())
                        .kv(DEPLOYMENT_TYPE_KEY_NAME, Coerce.toString(typeValue))
                        .log("Quarantining deployment status with an unknown deployment type");
                getQuarantine().add(String.valueOf(Coerce.toString(typeValue)), record);
                removeRecord(processedDeployments, record);
            }
        }
    }

    private List<PersistedStatusRecord> readPersistedRecords(Topics processedDeployments, DeploymentType type) {
        List<PersistedStatusRecord> records = new ArrayList<>();
        processedDeployments.forEach(node -> {
            if (!(node instanceof Topics)) {
                return;
            }
            Topics deploymentDetails = (Topics) node;
            DeploymentType deploymentType =
                    Coerce.toEnum(DeploymentType.class, deploymentDetails.find(DEPLOYMENT_TYPE_KEY_NAME));
            if (!Objects.equals(deploymentType, type)) {
                return;
            }
            records.add(new PersistedStatusRecord(deploymentDetails.getName(),
                    sequenceOf(deploymentDetails), legacyTimestamp(deploymentDetails.getName()),
                    deploymentDetails.toPOJO()));
        });
        return records;
    }

    private static Long sequenceOf(Topics deploymentDetails) {
        Object sequenceValue = deploymentDetails.find(DeploymentStatusReplayPlan.SEQUENCE_KEY_NAME);
        return sequenceValue == null ? null : Coerce.toLong(sequenceValue);
    }

    /**
     * Extract the millisecond timestamp that older versions encoded in the record's config key. Such records carry no
     * sequence number and are replayed before all sequenced records.
     */
    private static long legacyTimestamp(String key) {
        if (key == null || key.startsWith(SEQUENCE_KEY_PREFIX)) {
            return 0L;
        }
        try {
            return Long.parseLong(key);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private void warnAboutOverriddenTerminalStatuses(DeploymentType type, Plan plan) {
        for (TerminalOverride override : plan.getTerminalOverrides()) {
            logger.atWarn().kv(DEPLOYMENT_TYPE_LOG_KEY, type).kv(STATUS_KEY_LOG_KEY, override.getKeptRecord().getKey())
                    .kv(DEPLOYMENT_STATUS_KEY_NAME, override.getKeptRecord().getStatus())
                    .kv("overriddenStatusKey", override.getOverriddenRecord().getKey())
                    .kv("overriddenStatus", override.getOverriddenRecord().getStatus())
                    .log("Deployment status was reported after its terminal status; keeping the terminal status");
        }
    }

    private void removeRecord(Topics processedDeployments, PersistedStatusRecord record) {
        Topics node = processedDeployments.findTopics(record.getKey());
        if (node != null) {
            processedDeployments.remove(node);
        }
    }

    /**
     * Gets the list of callback functions based on the Deployment Type.
     *
     * @param type the type of deployment. {@link DeploymentType}
     * @return list of callback functions.
     */
    protected List<Function<Map<String, Object>, Boolean>> getConsumersForDeploymentType(DeploymentType type) {
        Map<String, Function<Map<String, Object>, Boolean>> stringFunctionMap = deploymentStatusConsumerMap.get(type);
        if (stringFunctionMap != null) {
            return new ArrayList<>(stringFunctionMap.values());
        }
        return Collections.emptyList();
    }

    /**
     * Get a reference to persisted deployment states Topics. Not thread-safe.
     *
     * @return Topics of persisted deployment states
     */
    protected Topics getProcessedDeployments() {
        if (processedDeployments == null) {
            processedDeployments = deploymentService.getRuntimeConfig().lookupTopics(PROCESSED_DEPLOYMENTS_TOPICS);
        }
        return processedDeployments;
    }

    /**
     * Get the quarantine store for records that can never be reported. Not thread-safe.
     *
     * @return quarantine store
     */
    synchronized DeploymentStatusQuarantine getQuarantine() {
        if (quarantine == null) {
            quarantine = new DeploymentStatusQuarantine(
                    deploymentService.getRuntimeConfig().lookupTopics(DeploymentStatusQuarantine.QUARANTINE_TOPIC));
        }
        return quarantine;
    }

    private long nextSequence() {
        if (sequenceSource != null) {
            return sequenceSource.getAsLong();
        }
        return getSequence().next();
    }

    // Synchronized because the counter is shared by all deployment types while persist calls hold different type
    // locks: two types racing here must not each build a counter and hand out the same sequence.
    private synchronized DeploymentStatusSequence getSequence() {
        if (sequence == null) {
            sequence = new DeploymentStatusSequence(deploymentService.getRuntimeConfig(), getProcessedDeployments());
        }
        return sequence;
    }
}
