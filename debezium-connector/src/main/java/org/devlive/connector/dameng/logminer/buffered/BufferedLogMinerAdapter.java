/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.buffered;

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
 * An implementation of {@link AbstractLogMinerStreamingAdapter} for capturing changes from LogMiner
 * using heap and off-heap cache/buffer mechanisms while reading LogMiner data in uncommitted mode.
 *
 * @author Chris Cranford
 */
public class BufferedLogMinerAdapter extends AbstractLogMinerStreamingAdapter {

    public static final String TYPE = "logminer";

    public BufferedLogMinerAdapter(DamengConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public OffsetContext.Loader<DamengOffsetContext> getOffsetContextLoader() {
        return new BufferedLogMinerDamengOffsetContextLoader(connectorConfig);
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
        return new BufferedLogMinerStreamingChangeEventSource(
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
    public DamengOffsetContext copyOffset(DamengConnectorConfig connectorConfig, DamengOffsetContext offsetContext) {
        return new BufferedLogMinerDamengOffsetContextLoader(connectorConfig).load(offsetContext.getOffset());
    }

}
