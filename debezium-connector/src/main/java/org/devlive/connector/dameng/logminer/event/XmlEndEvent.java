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
 * An event that represents the end of an XML document in the event stream.
 *
 * @author Chris Cranford
 */
public class XmlEndEvent extends LogMinerEvent {

    public XmlEndEvent(LogMinerEventRow row) {
        super(row);
    }

    public XmlEndEvent(EventType eventType, Scn scn, TableId tableId, String rowId, String rsId, Instant changeTime) {
        super(eventType, scn, tableId, rowId, rsId, changeTime);
    }

}
