/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.xstream;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.document.Document;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import org.devlive.connector.dameng.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Optional;

/**
 * The streaming adapter implementation for Dameng XStream.
 *
 * @author Chris Cranford
 */
public class XStreamAdapter extends AbstractStreamingAdapter<XStreamStreamingChangeEventSourceMetrics> {

    private static final Logger LOGGER = LoggerFactory.getLogger(XStreamAdapter.class);

    public static final String TYPE = "xstream";

    public XStreamAdapter(DamengConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            public boolean isPositionAtOrBefore(Document recorded, Document desired) {
                final LcrPosition recordedPosition = LcrPosition.valueOf(recorded.getString(SourceInfo.LCR_POSITION_KEY));
                final LcrPosition desiredPosition = LcrPosition.valueOf(desired.getString(SourceInfo.LCR_POSITION_KEY));
                final Scn recordedScn = recordedPosition != null ? recordedPosition.getScn() : resolveScn(recorded);
                final Scn desiredScn = desiredPosition != null ? desiredPosition.getScn() : resolveScn(desired);
                if (recordedPosition != null && desiredPosition != null) {
                    return recordedPosition.compareTo(desiredPosition) < 1;
                }
                return recordedScn.compareTo(desiredScn) < 1;
            }
        };
    }

    @Override
    public OffsetContext.Loader<DamengOffsetContext> getOffsetContextLoader() {
        return new XStreamDamengOffsetContextLoader(connectorConfig);
    }

    @Override
    public StreamingChangeEventSource<DamengPartition, DamengOffsetContext> getSource(DamengConnection connection,
                                                                                      EventDispatcher<DamengPartition, TableId> dispatcher,
                                                                                      ErrorHandler errorHandler,
                                                                                      Clock clock,
                                                                                      DamengDatabaseSchema schema,
                                                                                      DamengTaskContext taskContext,
                                                                                      Configuration jdbcConfig,
                                                                                      XStreamStreamingChangeEventSourceMetrics streamingMetrics,
                                                                                      SnapshotterService snapshotterService) {
        return new XstreamStreamingChangeEventSource(
                connectorConfig,
                connection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                streamingMetrics);
    }

    @Override
    public XStreamStreamingChangeEventSourceMetrics getStreamingMetrics(DamengTaskContext taskContext,
                                                                        ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                        EventMetadataProvider metadataProvider,
                                                                        DamengConnectorConfig connectorConfig) {
        return new XStreamStreamingChangeEventSourceMetrics(taskContext, changeEventQueueMetrics, metadataProvider);
    }

    @Override
    public TableNameCaseSensitivity getTableNameCaseSensitivity(DamengConnection connection) {
        // Always use tablename case insensitivity true when on Dameng 11, otherwise false.
        if (connection.getDamengVersion().getMajor() == 11) {
            return TableNameCaseSensitivity.SENSITIVE;
        }
        return super.getTableNameCaseSensitivity(connection);
    }

    @Override
    public DamengOffsetContext determineSnapshotOffset(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> ctx,
                                                       DamengConnectorConfig connectorConfig,
                                                       DamengConnection connection)
            throws SQLException {

        final Optional<Scn> latestTableDdlScn = getLatestTableDdlScn(ctx, connection);

        // we must use an SCN for taking the snapshot that represents a later timestamp than the latest DDL change than
        // any of the captured tables; this will not be a problem in practice, but during testing it may happen that the
        // SCN of "now" represents the same timestamp as a newly created table that should be captured; in that case
        // we'd get a ORA-01466 when running the flashback query for doing the snapshot
        Scn currentScn = null;
        do {
            currentScn = connection.getCurrentScn();
        } while (areSameTimestamp(latestTableDdlScn.orElse(null), currentScn, connection));

        LOGGER.info("\tCurrent SCN resolved as {}", currentScn);

        return DamengOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(currentScn)
                .snapshotScn(currentScn)
                .snapshotPendingTransactions(Collections.emptyMap())
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>())
                .build();
    }

    @Override
    public Scn getOffsetScn(DamengOffsetContext offsetContext) {

        final byte[] startPosition;
        String lcrPosition = offsetContext.getLcrPosition();
        if (lcrPosition != null) {
            startPosition = LcrPosition.valueOf(lcrPosition).getRawPosition();
            return getScn(startPosition);
        }
        return offsetContext.getScn();
    }

    private static Scn getScn(byte[] startPosition) {
        try {
            return new Scn(XStreamUtility.getSCNFromPosition(startPosition).bigIntegerValue());
        }
        catch (StreamsException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DamengOffsetContext copyOffset(DamengConnectorConfig connectorConfig, DamengOffsetContext offsetContext) {
        return new XStreamDamengOffsetContextLoader(connectorConfig).load(offsetContext.getOffset());
    }

}
