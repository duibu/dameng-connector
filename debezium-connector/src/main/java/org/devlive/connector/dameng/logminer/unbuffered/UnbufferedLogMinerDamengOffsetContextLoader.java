/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.unbuffered;

import io.debezium.common.annotation.Incubating;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import org.devlive.connector.dameng.CommitScn;
import org.devlive.connector.dameng.DamengConnectorConfig;
import org.devlive.connector.dameng.DamengOffsetContext;
import org.devlive.connector.dameng.SourceInfo;

import java.util.Map;

/**
 * An {@link OffsetContext.Loader} implementation for the unbuffered Dameng LogMiner adapter.
 *
 * @author Chris Cranford
 */
@Incubating
public class UnbufferedLogMinerDamengOffsetContextLoader implements OffsetContext.Loader<DamengOffsetContext> {

    private final DamengConnectorConfig connectorConfig;

    public UnbufferedLogMinerDamengOffsetContextLoader(DamengConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public DamengOffsetContext load(Map<String, ?> offset) {
        return DamengOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(DamengOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY))
                .commitScn(CommitScn.load(offset))
                .snapshotScn(DamengOffsetContext.loadSnapshotScn(offset))
                .snapshotPendingTransactions(DamengOffsetContext.loadSnapshotPendingTransactions(offset))
                .snapshot(loadSnapshot(offset).orElse(null))
                .snapshotCompleted(loadSnapshotCompleted(offset))
                .transactionContext(TransactionContext.load(offset))
                .incrementalSnapshotContext(SignalBasedIncrementalSnapshotContext.load(offset))
                .transactionId(DamengOffsetContext.loadTransactionId(offset))
                .transactionSequence(DamengOffsetContext.loadTransactionSequence(offset))
                .build();
    }
}
