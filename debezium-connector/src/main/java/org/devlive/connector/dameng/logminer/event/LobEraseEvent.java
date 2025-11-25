/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.event;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

import java.time.Instant;

/**
 * A LogMiner event that represents a {@code LOB_ERASE} operation.
 *
 * @author Chris Cranford
 */
public class LobEraseEvent extends LogMinerEvent {
    public LobEraseEvent(LogMinerEventRow row) {
        super(row);
    }

    public LobEraseEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime) {
        super(eventType, scn, tableId, rowId, rsId, changeTime);
    }
}
