/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.event;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.relational.TableId;

import java.time.Instant;

/**
 * An event that represents the start of an XML document in the event stream.
 *
 * @author Chris Cranford
 */
public class XmlBeginEvent extends DmlEvent {

    private final String columnName;

    public XmlBeginEvent(LogMinerEventRow row, LogMinerDmlEntry dmlEntry, String columnName) {
        super(row, dmlEntry);
        this.columnName = columnName;
    }

    public XmlBeginEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime,
                         LogMinerDmlEntry dmlEntry, String columnName) {
        super(eventType, scn, tableId, rowId, rsId, changeTime, dmlEntry);
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public String toString() {
        return "XmlBeginEvent{" +
                "columnName='" + columnName + '\'' +
                "} " + super.toString();
    }
}
