/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.xstream;

import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import org.devlive.connector.dameng.DamengConnectorConfig;
import org.devlive.connector.dameng.DamengOffsetContext;
import org.devlive.connector.dameng.Scn;
import org.devlive.connector.dameng.SourceInfo;

import java.util.Map;

/**
 * The {@link OffsetContext} loader implementation for the Dameng XStream adapter
 *
 * @author Chris Cranford
 */
public class XStreamDamengOffsetContextLoader implements OffsetContext.Loader<DamengOffsetContext> {

    private final DamengConnectorConfig connectorConfig;

    public XStreamDamengOffsetContextLoader(DamengConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public DamengOffsetContext load(Map<String, ?> offset) {
        return DamengOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(resolveScn(offset))
                .lcrPosition(loadLcrPosition(offset))
                .snapshotScn(DamengOffsetContext.loadSnapshotScn(offset))
                .snapshotPendingTransactions(DamengOffsetContext.loadSnapshotPendingTransactions(offset))
                .snapshot(loadSnapshot(offset).orElse(null))
                .snapshotCompleted(loadSnapshotCompleted(offset))
                .transactionContext(TransactionContext.load(offset))
                .incrementalSnapshotContext(SignalBasedIncrementalSnapshotContext.load(offset))
                .build();
    }

    private Scn resolveScn(Map<String, ?> offset) {
        final String lcrPosition = loadLcrPosition(offset);
        return lcrPosition != null
                ? LcrPosition.valueOf(lcrPosition).getScn()
                : DamengOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
    }

    private String loadLcrPosition(Map<String, ?> offset) {
        return (String) offset.get(SourceInfo.LCR_POSITION_KEY);
    }
}
