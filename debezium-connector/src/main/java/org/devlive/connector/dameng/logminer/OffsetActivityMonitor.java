/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer;

import org.devlive.connector.dameng.DamengOffsetContext;
import org.devlive.connector.dameng.Scn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A utility class that provides methods for tracking state changes to the connector's offsets.
 *
 * @author Chris Cranford
 */
public class OffsetActivityMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetActivityMonitor.class);

    private final int staleMaxIterations;
    private final DamengOffsetContext offsetContext;
    private final LogMinerStreamingChangeEventSourceMetrics metrics;

    private int unchangedScnCount;
    private Scn previousOffsetScn = Scn.NULL;
    private Map<Integer, Scn> previousCommitScns = new HashMap<>();

    public OffsetActivityMonitor(int staleMaxIterations, DamengOffsetContext offsetContext, LogMinerStreamingChangeEventSourceMetrics metrics) {
        this.staleMaxIterations = staleMaxIterations;
        this.offsetContext = offsetContext;
        this.metrics = metrics;
    }

    public void checkForStaleOffsets() {
        // Check for stale state
        if (offsetContext.getCommitScn() != null) {
            final Scn currentScn = offsetContext.getScn();
            final Map<Integer, Scn> currentCommitScns = offsetContext.getCommitScn().getCommitScnForAllRedoThreads();
            if (previousOffsetScn.equals(currentScn) && !previousOffsetScn.equals(currentCommitScns)) {
                unchangedScnCount++;

                if (unchangedScnCount == staleMaxIterations) {
                    LOGGER.warn("Offset SCN {} has not changed in {} mining session iterations. " +
                            "This indicates long running transaction(s) are active. Commit SCNs {}.",
                            previousOffsetScn, staleMaxIterations, previousCommitScns);
                    metrics.incrementScnFreezeCount();
                    unchangedScnCount = 0;
                }
            }
            else {
                metrics.setScnFreezeCount(0);
                unchangedScnCount = 0;
            }
        }

        // Update tracked stats
        previousOffsetScn = offsetContext.getScn();
        if (offsetContext.getCommitScn() != null) {
            previousCommitScns = offsetContext.getCommitScn().getCommitScnForAllRedoThreads();
        }
    }

}
