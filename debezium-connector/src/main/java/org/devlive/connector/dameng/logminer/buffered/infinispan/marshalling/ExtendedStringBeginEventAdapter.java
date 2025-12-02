/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.buffered.infinispan.marshalling;

import io.debezium.relational.TableId;
import org.devlive.connector.dameng.Scn;
import org.devlive.connector.dameng.logminer.event.EventType;
import org.devlive.connector.dameng.logminer.event.ExtendedStringBeginEvent;
import org.devlive.connector.dameng.logminer.event.SelectLobLocatorEvent;
import org.devlive.connector.dameng.logminer.parser.LogMinerDmlEntryImpl;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

import java.time.Instant;

/**
 * An Infinispan ProtoStream adapter to marshall {@link ExtendedStringBeginEvent} instances.
 *
 * This class defines a factory for creating {@link ExtendedStringBeginEvent} instances when hydrating
 * records from the persisted datastore as well as field handlers to extract values to be marshalled
 * to the protocol buffer stream.
 *
 * @author Chris Cranford
 */
@ProtoAdapter(ExtendedStringBeginEvent.class)
public class ExtendedStringBeginEventAdapter extends DmlEventAdapter {
    /**
     * A ProtoStream factory that creates {@link ExtendedStringBeginEvent} instances.
     *
     * @param eventType the event type
     * @param scn the system change number, must not be {@code null}
     * @param tableId the fully-qualified table name
     * @param rowId the Oracle row-id the change is associated with
     * @param rsId the Oracle rollback segment identifier
     * @param changeTime the time the change occurred
     * @param entry the parsed SQL statement entry
     * @param columnName the column name references by the SelectLobLocatorEvent
     * @return the constructed ExtendedStringBeginEvent instance
     */
    @ProtoFactory
    public ExtendedStringBeginEvent factory(int eventType, String scn, String tableId, String rowId, String rsId, String changeTime, LogMinerDmlEntryImpl entry,
                                            String columnName) {
        return new ExtendedStringBeginEvent(EventType.from(eventType), Scn.valueOf(scn), TableId.parse(tableId), rowId, rsId, Instant.parse(changeTime), entry,
                columnName);
    }

    /**
     * A ProtoStream handler to extract the {@code columnName} field from a {@link SelectLobLocatorEvent} type.
     *
     * @param event the event instance, must not be {@code null}
     * @return the column name
     */
    @ProtoField(number = 8)
    public String getColumnName(ExtendedStringBeginEvent event) {
        return event.getColumnName();
    }
}
