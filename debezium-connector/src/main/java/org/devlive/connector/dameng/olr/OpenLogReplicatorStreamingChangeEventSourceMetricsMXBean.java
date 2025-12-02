/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.olr;


import org.devlive.connector.dameng.DamengCommonStreamingChangeEventSourceMetricsMXBean;

import java.math.BigInteger;

/**
 * Dameng Streaming Metrics for OpenLogReplicator.
 *
 * @author Chris Cranford
 */
public interface OpenLogReplicatorStreamingChangeEventSourceMetricsMXBean
        extends DamengCommonStreamingChangeEventSourceMetricsMXBean {

    /**
     * @return checkpoint scn where the connector resumes on restart
     */
    BigInteger getCheckpointScn();

    /**
     * @return checkpoint index, resume position within a checkpoint block
     */
    long getCheckpointIndex();

    /**
     * @return number of events processed from OpenLogReplicator
     */
    long getProcessedEventCount();

}
