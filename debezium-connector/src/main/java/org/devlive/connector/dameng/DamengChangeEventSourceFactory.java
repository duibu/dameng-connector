/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.debezium.config.Configuration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

import java.util.Optional;

@SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "DLS_DEAD_LOCAL_STORE"})
public class DamengChangeEventSourceFactory implements ChangeEventSourceFactory<DamengPartition, DamengOffsetContext> {
    private final DamengConnectorConfig configuration;
    private final DamengConnectionFactory connectionFactory;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<DamengPartition, TableId> dispatcher;
    private final Clock clock;
    private final DamengDatabaseSchema schema;
    private final Configuration jdbcConfig;
    private final DamengTaskContext taskContext;
    private final AbstractDamengStreamingChangeEventSourceMetrics streamingMetrics;
    private final SnapshotterService snapshotterService;

    public DamengChangeEventSourceFactory(DamengConnectorConfig configuration, DamengConnectionFactory connectionFactory,
                                          ErrorHandler errorHandler, EventDispatcher<DamengPartition, TableId> dispatcher, Clock clock, DamengDatabaseSchema schema,
                                          Configuration jdbcConfig, DamengTaskContext taskContext,
                                          AbstractDamengStreamingChangeEventSourceMetrics streamingMetrics, SnapshotterService snapshotterService) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.jdbcConfig = jdbcConfig;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
        this.snapshotterService = snapshotterService;
    }


    @Override
    public SnapshotChangeEventSource<DamengPartition, DamengOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<DamengPartition> snapshotProgressListener, NotificationService<DamengPartition, DamengOffsetContext> notificationService) {
        return new DamengSnapshotChangeEventSource(configuration, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService,
                snapshotterService);
    }

    @Override
    public StreamingChangeEventSource<DamengPartition, DamengOffsetContext> getStreamingChangeEventSource() {
        return configuration.getAdapter().getSource(
                connectionFactory.mainConnection(),
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                jdbcConfig,
                streamingMetrics,
                snapshotterService);
    }


    @Override
    public Optional<IncrementalSnapshotChangeEventSource<DamengPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
            DamengOffsetContext offsetContext,
            SnapshotProgressListener<DamengPartition> snapshotProgressListener,
            DataChangeEventListener<DamengPartition> dataChangeEventListener,
            NotificationService<DamengPartition, DamengOffsetContext> notificationService) {
        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        if (Strings.isNullOrEmpty(configuration.getSignalingDataCollectionId())) {
            return Optional.empty();
        }

        // Incremental snapshots requires a secondary database connection
        // This is because Xstream does not allow any work on the connection while the LCR handler may be invoked
        // and LogMiner streams results from the CDB$ROOT container but we will need to stream changes from the
        // PDB when reading snapshot records.
        return Optional.of(new DamengSignalBasedIncrementalSnapshotChangeEventSource(
                configuration,
                new DamengConnection(connectionFactory.mainConnection().config()),
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService));
    }
}
