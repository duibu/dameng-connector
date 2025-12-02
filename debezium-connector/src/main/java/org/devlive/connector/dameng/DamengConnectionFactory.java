/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.jdbc.ConnectionFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.util.Strings;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Factory to manage between read-only and regular connection for the purpose of using only what is necessary for
 * start the connector while the read-only will be used to log mining.

 * @author Lucas Gazire
 * @author Chris Cranford
 */
public class DamengConnectionFactory implements MainConnectionProvidingConnectionFactory<DamengConnection> {

    private final ConnectionFactory<DamengConnection> mainConnectionFactory;
    private final ConnectionFactory<DamengConnection> readOnlyConnectionFactory;
    private final DamengConnection mainConnection;
    private final DamengConnection readOnlyConnection;

    public DamengConnectionFactory(DamengConnectorConfig connectorConfig) {
        this.mainConnectionFactory = () -> new DamengConnection(connectorConfig.getJdbcConfig());
        this.readOnlyConnectionFactory = buildReadOnlyConnectionFactory(connectorConfig);
        this.mainConnection = mainConnectionFactory.newConnection();
        this.readOnlyConnection = readOnlyConnectionFactory != null ? readOnlyConnectionFactory.newConnection() : null;
    }

    public DamengConnection getConnection() {
        return readOnlyConnection != null ? readOnlyConnection : mainConnection;
    }

    @Override
    public DamengConnection mainConnection() {
        return mainConnection;
    }

    @Override
    public DamengConnection newConnection() {
        return mainConnectionFactory.newConnection();
    }

    private static ConnectionFactory<DamengConnection> buildReadOnlyConnectionFactory(DamengConnectorConfig connectorConfig) {
        return connectorConfig.isLogMiningReadOnly() ? new ReadOnlyConnectionFactory(connectorConfig) : null;
    }

    /**
     * A connection factory that produces {@link DamengConnection} instances that are explicitly marked
     * to use a read-only connection.
     */
    private static class ReadOnlyConnectionFactory implements ConnectionFactory<DamengConnection> {

        private final ConnectionFactory<DamengConnection> delegate;

        ReadOnlyConnectionFactory(DamengConnectorConfig connectorConfig) {
            this.delegate = () -> new DamengConnection(buildReadOnlyConfig(connectorConfig)) {
                @Override
                public synchronized Connection connection(boolean executeOnConnect) throws SQLException {
                    final Connection connection = super.connection(executeOnConnect);
                    connection.setReadOnly(true);
                    return connection;
                }
            };
        }

        @Override
        public DamengConnection newConnection() {
            return delegate.newConnection();
        }

        private static JdbcConfiguration buildReadOnlyConfig(DamengConnectorConfig connectorConfig) {
            if (Strings.isNullOrEmpty(connectorConfig.getReadonlyHostname())) {
                throw new DebeziumException("Cannot create read only connection, read only hostname is empty");
            }

            final Map<String, String> jdbcConfig = connectorConfig.getJdbcConfig().asMap();
            jdbcConfig.put("hostname", connectorConfig.getReadonlyHostname());
            return JdbcConfiguration.adapt(Configuration.from(jdbcConfig));
        }
    }
}