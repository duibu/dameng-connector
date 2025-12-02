/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package org.devlive.connector.dameng.logminer.event;

import io.debezium.relational.TableId;
import org.devlive.connector.dameng.Scn;
import org.devlive.connector.dameng.logminer.parser.LogMinerDmlEntry;

import java.time.Instant;

public class TruncateEvent extends DmlEvent {
    public TruncateEvent(LogMinerEventRow row, LogMinerDmlEntry dmlEntry) {
        super(row, dmlEntry);
    }

    public TruncateEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId,
                         Instant changeTime, LogMinerDmlEntry dmlEntry) {
        super(eventType, scn, tableId, rowId, rsId, changeTime, dmlEntry);
    }
}
