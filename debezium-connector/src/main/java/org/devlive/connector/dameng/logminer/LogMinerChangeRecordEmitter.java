/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.debezium.DebeziumException;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Table;
import io.debezium.util.Clock;
import org.devlive.connector.dameng.BaseChangeRecordEmitter;
import org.devlive.connector.dameng.DamengConnectorConfig;
import org.devlive.connector.dameng.DamengDatabaseSchema;
import org.devlive.connector.dameng.logminer.event.EventType;
import org.devlive.connector.dameng.logminer.valueholder.LogMinerColumnValue;
import org.devlive.connector.dameng.logminer.valueholder.LogMinerDmlEntry;

import java.util.Arrays;
import java.util.List;

/**
 * Emits change record based on a single {@link LogMinerDmlEntry} event.
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2"})
public class LogMinerChangeRecordEmitter<P extends Partition>
        extends BaseChangeRecordEmitter<Object> {
    private final Operation operation;

    public LogMinerChangeRecordEmitter(
            DamengConnectorConfig connectorConfig,
            P partition,
            OffsetContext offset,
            Operation operation,
            Object[] oldValues,
            Object[] newValues,
            Table table,
            DamengDatabaseSchema schema,
            Clock clock
    ) {
        super(connectorConfig, partition, offset, schema, table, clock, oldValues, newValues);
        this.operation = operation;
//        this.dmlEntry = dmlEntry;
//        this.table = table;
    }

    public LogMinerChangeRecordEmitter(DamengConnectorConfig connectorConfig,
                                       P partition,
                                       OffsetContext offset,
                                       EventType eventType,
                                       Object[] oldValues,
                                       Object[] newValues,
                                       Table table,
                                       DamengDatabaseSchema schema,
                                       Clock clock) {
        this(connectorConfig, partition, offset, getOperation(eventType), oldValues, newValues, table, schema, clock);
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    
    private static Operation getOperation(EventType eventType) {
        switch (eventType) {
            case INSERT:
                return Operation.CREATE;
            case UPDATE:
            case SELECT_LOB_LOCATOR:
            case EXTENDED_STRING_BEGIN:
            case XML_BEGIN:
                return Operation.UPDATE;
            case DELETE:
                return Operation.DELETE;
            default:
                throw new DebeziumException("Unsupported operation type: " + eventType);
        }
    }
    
}
