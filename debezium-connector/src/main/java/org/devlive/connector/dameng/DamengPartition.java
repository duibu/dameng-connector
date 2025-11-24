package org.devlive.connector.dameng;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DamengPartition extends AbstractPartition implements Partition {

    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    public DamengPartition(String serverName, String databaseName) {
        super(databaseName);
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final DamengPartition other = (DamengPartition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return serverName.hashCode();
    }

    @Override
    public String toString() {
        return "DamengPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<DamengPartition> {
        private final DamengConnectorConfig connectorConfig;

        Provider(DamengConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<DamengPartition> getPartitions() {
            final String databaseName = Strings.isNullOrBlank(connectorConfig.getPdbName())
                    ? connectorConfig.getDatabaseName()
                    : connectorConfig.getPdbName();
            return Collections.singleton(new DamengPartition(connectorConfig.getLogicalName(), databaseName));
        }
    }
    
}
