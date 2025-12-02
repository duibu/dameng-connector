/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;

import java.sql.SQLException;

/**
 * @author Chris Cranford
 */
public class DamengSignalBasedIncrementalSnapshotChangeEventSource extends SignalBasedIncrementalSnapshotChangeEventSource<DamengPartition, TableId> {

    private final String pdbName;
    private final DamengConnection connection;

    public DamengSignalBasedIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                                 JdbcConnection jdbcConnection,
                                                                 EventDispatcher<DamengPartition, TableId> dispatcher,
                                                                 DatabaseSchema<?> databaseSchema,
                                                                 Clock clock,
                                                                 SnapshotProgressListener<DamengPartition> progressListener,
                                                                 DataChangeEventListener<DamengPartition> dataChangeEventListener,
                                                                 NotificationService<DamengPartition, DamengOffsetContext> notificationService) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        this.pdbName = ((DamengConnectorConfig) config).getPdbName();
        this.connection = (DamengConnection) jdbcConnection;
    }

    @Override
    protected String getSignalTableName(String dataCollectionId) {
        final TableId tableId = DamengTableIdParser.parse(dataCollectionId);
        return DamengTableIdParser.quoteIfNeeded(tableId, false, true, ((DamengConnection) jdbcConnection).getSQLKeywords());
    }

    @Override
    protected void preReadChunk(IncrementalSnapshotContext<TableId> context) {
        super.preReadChunk(context);

        if (pdbName != null) {
            connection.setSessionToPdb(pdbName);
        }
    }

    @Override
    protected void postReadChunk(IncrementalSnapshotContext<TableId> context) {
        super.postReadChunk(context);

        if (pdbName != null) {
            connection.resetSessionToCdb();
        }
    }

    @Override
    protected void postIncrementalSnapshotCompleted() {
        super.postIncrementalSnapshotCompleted();

        try {
            connection.close();
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to close snapshot connection", e);
        }
    }

    @Override
    protected String getTableDDL(TableId dataCollectionId) throws SQLException {
        this.connection.setAutoCommit(false);
        String ddlString = this.connection.getTableMetadataDdl(dataCollectionId);
        this.connection.setAutoCommit(true);
        return ddlString;
    }
}
