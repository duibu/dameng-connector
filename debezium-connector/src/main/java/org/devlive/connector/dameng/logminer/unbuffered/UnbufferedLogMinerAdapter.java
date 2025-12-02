/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.unbuffered;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import org.devlive.connector.dameng.*;
import org.devlive.connector.dameng.logminer.AbstractLogMinerStreamingAdapter;
import org.devlive.connector.dameng.logminer.LogMinerStreamingChangeEventSourceMetrics;

/**
 * An Dameng LogMiner {@link StreamingAdapter} implementation that relies on
 * Dameng LogMiner's {@code COMMITTED_DATA_ONLY} mode to capture changes without requiring that the
 * connector buffer large transactions.
 *
 * @author Chris Cranford
 */
@Incubating
public class UnbufferedLogMinerAdapter extends AbstractLogMinerStreamingAdapter {

    public static final String TYPE = "logminer_unbuffered";

    public UnbufferedLogMinerAdapter(DamengConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public StreamingChangeEventSource<DamengPartition, DamengOffsetContext> getSource(DamengConnection connection,
                                                                                      EventDispatcher<DamengPartition, TableId> dispatcher,
                                                                                      ErrorHandler errorHandler,
                                                                                      Clock clock,
                                                                                      DamengDatabaseSchema schema,
                                                                                      DamengTaskContext taskContext,
                                                                                      Configuration jdbcConfig,
                                                                                      LogMinerStreamingChangeEventSourceMetrics streamingMetrics,
                                                                                      SnapshotterService snapshotterService) {
        return new UnbufferedLogMinerStreamingChangeEventSource(
                connectorConfig,
                connection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                jdbcConfig,
                streamingMetrics);
    }

    @Override
    public OffsetContext.Loader<DamengOffsetContext> getOffsetContextLoader() {
        return new UnbufferedLogMinerDamengOffsetContextLoader(connectorConfig);
    }

    @Override
    public DamengOffsetContext copyOffset(DamengConnectorConfig connectorConfig, DamengOffsetContext offsetContext) {
        return new UnbufferedLogMinerDamengOffsetContextLoader(connectorConfig).load(offsetContext.getOffset());
    }
}
