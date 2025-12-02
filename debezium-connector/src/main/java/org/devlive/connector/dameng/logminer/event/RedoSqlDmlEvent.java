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

/**
 * A specialization of {@link DmlEvent} that also stores the LogMiner REDO SQL statements.
 *
 * @author Chris Cranford
 */
public class RedoSqlDmlEvent extends DmlEvent {

    private String redoSql;

    public RedoSqlDmlEvent(LogMinerEventRow row, LogMinerDmlEntry dmlEntry, String redoSql) {
        super(row, dmlEntry);
        this.redoSql = redoSql;
    }

    public RedoSqlDmlEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime,
                           LogMinerDmlEntry dmlEntry, String redoSql) {
        super(eventType, scn, tableId, rowId, rsId, changeTime, dmlEntry);
        this.redoSql = redoSql;
    }

    public String getRedoSql() {
        return redoSql;
    }

}
