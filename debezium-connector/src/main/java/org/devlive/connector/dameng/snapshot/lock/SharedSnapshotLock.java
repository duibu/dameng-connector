/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.snapshot.lock;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.snapshot.spi.SnapshotLock;
import org.devlive.connector.dameng.DamengConnector;
import org.devlive.connector.dameng.DamengConnectorConfig;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@ConnectorSpecific(connector = DamengConnector.class)
public class SharedSnapshotLock implements SnapshotLock {

    @Override
    public String name() {
        return DamengConnectorConfig.SnapshotLockingMode.SHARED.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {

        return Optional.of(String.format("LOCK TABLE %s IN ROW SHARE MODE", tableId));
    }
}
