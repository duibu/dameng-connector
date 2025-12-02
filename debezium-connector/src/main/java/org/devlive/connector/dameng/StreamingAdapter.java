package org.devlive.connector.dameng;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

import java.sql.SQLException;

public interface  StreamingAdapter<T extends AbstractDamengStreamingChangeEventSourceMetrics> {
    /**
     * Controls whether table names are viewed as case-sensitive or not.
     */
    enum TableNameCaseSensitivity {
        /**
         * Sensitive case implies that the table names are taken from the JDBC driver and kept as-is
         * in the in-memory relational objects.  Any {@link TableId} that is obtained will always
         * have a table-name in the case that the driver provided.  This is the default behavior
         * for almost all cases.
         */
        SENSITIVE,

        /**
         * Insensitive case implies that the table names are taken from the JDBC driver and converted
         * to lower-case in the in-memory relational objects.  Any {@link TableId} that is obtained
         * will always have a table-name in lower case regardless of how it may be represented in
         * the database.
         */
        INSENSITIVE
    };

    String getType();

    HistoryRecordComparator getHistoryRecordComparator();

    OffsetContext.Loader<DamengOffsetContext> getOffsetContextLoader();

    StreamingChangeEventSource<DamengPartition, DamengOffsetContext> getSource(DamengConnection connection,
                                                                               EventDispatcher<DamengPartition, TableId> dispatcher,
                                                                               ErrorHandler errorHandler, Clock clock,
                                                                               DamengDatabaseSchema schema,
                                                                               DamengTaskContext taskContext,
                                                                               Configuration jdbcConfig,
                                                                               T streamingMetrics, SnapshotterService snapshotterService);

    T getStreamingMetrics(DamengTaskContext taskContext,
                          ChangeEventQueueMetrics changeEventQueueMetrics,
                          EventMetadataProvider metadataProvider,
                          DamengConnectorConfig connectorConfig);

    /**
     * Returns whether table names are case sensitive.
     * <p>
     * By default the Oracle driver returns table names that are case sensitive.  The table names will
     * be returned in upper-case by default and will only be returned in lower or mixed case when the
     * table is created using double-quotes to preserve case.  The adapter aligns with the driver's
     * behavior and enforces that table names are case sensitive by default.
     *
     * @param connection database connection, should never be {@code null}
     * @return the case sensitivity setting for table names used by the connector's runtime adapter
     */
    default TableNameCaseSensitivity getTableNameCaseSensitivity(DamengConnection connection) {
        return TableNameCaseSensitivity.SENSITIVE;
    }

    /**
     * Returns the offset context based on the snapshot state.
     *
     * @param ctx             the relational snapshot context, should never be {@code null}
     * @param connectorConfig the connector configuration, should never be {@code null}
     * @param connection      the database connection, should never be {@code null}
     * @return the offset context, never {@code null}
     * @throws SQLException if a database error occurred
     */
    DamengOffsetContext determineSnapshotOffset(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<DamengPartition, DamengOffsetContext> ctx,
                                                DamengConnectorConfig connectorConfig, DamengConnection connection)
            throws SQLException;

    /**
     * Returns the value converter for the streaming adapter.
     *
     * @param connectorConfig the connector configuration, shoudl never be {@code null}
     * @param connection      the database connection, should never be {@code null}
     * @return the value converter to be used
     */
    default DamengValueConverters getValueConverter(DamengConnectorConfig connectorConfig, DamengConnection connection) {
        return new DamengValueConverters(connectorConfig, connection);
    }

    /**
     * Returns the Scn stored in the offset.
     *
     * @param offsetContext the connector offset context
     * @return the {@code Scn} stored in the offset
     */
    Scn getOffsetScn(DamengOffsetContext offsetContext);

    /**
     * Creates a copy of the existing offsets.
     *
     * @param connectorConfig the connector configuration, should never be {@code null}
     * @param offsetContext   the current offset context, should never be {@code null}
     * @return a copy of the offset context for this adapter
     */
    DamengOffsetContext copyOffset(DamengConnectorConfig connectorConfig, DamengOffsetContext offsetContext);
}
