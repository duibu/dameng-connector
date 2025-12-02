/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.metadata;

import io.debezium.config.Field;
import io.debezium.metadata.ConnectorDescriptor;
import io.debezium.metadata.ConnectorMetadata;
import org.devlive.connector.dameng.DamengConnector;
import org.devlive.connector.dameng.DamengConnectorConfig;
import org.devlive.connector.dameng.Module;

public class DamengConnectorMetadata implements ConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor(DamengConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getConnectorFields() {
        return DamengConnectorConfig.ALL_FIELDS;
    }

}
