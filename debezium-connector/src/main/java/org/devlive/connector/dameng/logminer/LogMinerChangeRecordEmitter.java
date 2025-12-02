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
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import oracle.jdbc.OracleTypes;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.devlive.connector.dameng.BaseChangeRecordEmitter;
import org.devlive.connector.dameng.DamengConnectorConfig;
import org.devlive.connector.dameng.DamengDatabaseSchema;
import org.devlive.connector.dameng.logminer.event.EventType;
import org.devlive.connector.dameng.logminer.valueholder.LogMinerDmlEntry;
import org.devlive.connector.dameng.util.TimestampUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

/**
 * Emits change record based on a single {@link LogMinerDmlEntry} event.
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2"})
public class LogMinerChangeRecordEmitter extends BaseChangeRecordEmitter<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerChangeRecordEmitter.class);

    private final Operation operation;

    public LogMinerChangeRecordEmitter(DamengConnectorConfig connectorConfig, Partition partition, OffsetContext offset,
                                       Operation operation, Object[] oldValues, Object[] newValues, Table table,
                                       DamengDatabaseSchema schema, Clock clock) {
        super(connectorConfig, partition, offset, schema, table, clock, oldValues, newValues);
        this.operation = operation;
    }

    public LogMinerChangeRecordEmitter(DamengConnectorConfig connectorConfig, Partition partition, OffsetContext offset,
                                       EventType eventType, Object[] oldValues, Object[] newValues, Table table,
                                       DamengDatabaseSchema schema, Clock clock) {
        this(connectorConfig, partition, offset, getOperation(eventType), oldValues, newValues, table, schema, clock);
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

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    protected Object convertReselectPrimaryKeyColumn(Connection connection, Column column, Object value) {
        if (value instanceof String) {
            // LogMiner raw values are always string; otherwise generally null
            switch (column.jdbcType()) {
                case OracleTypes.TIMESTAMP:
                case OracleTypes.DATE:
                    final String formattedTimestamp = TimestampUtils.toSqlCompliantFunctionCall((String) value);
                    if (!Strings.isNullOrBlank(formattedTimestamp)) {
                        value = convertValueViaQuery(connection, formattedTimestamp);
                    }
                    break;
                case OracleTypes.INTERVALYM:
                case OracleTypes.INTERVALDS:
                    // LogMiner provides these values in SQL-compliant query fragments
                    value = convertValueViaQuery(connection, (String) value);
                    break;
                default:
                    // no -op
                    break;
            }
        }
        return value;
    }
    
}
