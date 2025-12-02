/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.olr;

import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import org.devlive.connector.dameng.CommitScn;
import org.devlive.connector.dameng.DamengConnectorConfig;
import org.devlive.connector.dameng.DamengOffsetContext;
import org.devlive.connector.dameng.SourceInfo;

import java.util.Map;

/**
 * The {@link OffsetContext} loader implementation for OpenLogReplicator.
 *
 * @author Chris Cranford
 */
public class OpenLogReplicatorDamengOffsetContextLoader implements OffsetContext.Loader<DamengOffsetContext> {

    private final DamengConnectorConfig connectorConfig;

    public OpenLogReplicatorDamengOffsetContextLoader(DamengConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public DamengOffsetContext load(Map<String, ?> offset) {
        return DamengOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(DamengOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY))
                .scnIndex((Long) offset.get(SourceInfo.SCN_INDEX_KEY))
                .commitScn(CommitScn.empty())
                .snapshot(loadSnapshot(offset).orElse(null))
                .snapshotCompleted(loadSnapshotCompleted(offset))
                .transactionContext(TransactionContext.load(offset))
                .incrementalSnapshotContext(SignalBasedIncrementalSnapshotContext.load(offset))
                .build();
    }

}
