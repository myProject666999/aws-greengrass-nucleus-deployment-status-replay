/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.deployment.exceptions;

/**
 * Thrown when the IoT Jobs service explicitly rejected a job status update (the update landed on the
 * {@code update/rejected} topic). Such an update can never succeed, unlike a network or authorization failure which
 * may succeed on a later attempt.
 */
public class JobStatusUpdateRejectedException extends Exception {
    static final long serialVersionUID = -3387516993124229948L;

    public JobStatusUpdateRejectedException(String message) {
        super(message);
    }
}
