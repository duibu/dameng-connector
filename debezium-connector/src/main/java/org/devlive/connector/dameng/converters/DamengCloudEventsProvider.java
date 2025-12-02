/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.converters;

import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.SerializerType;
import org.devlive.connector.dameng.Module;

/**
 * An implementation of {@link CloudEventsProvider} for Oracle.
 *
 * @author Chris Cranford
 */
public class DamengCloudEventsProvider implements CloudEventsProvider {
    @Override
    public String getName() {
        return Module.name();
    }

    @Override
    public CloudEventsMaker createMaker(RecordAndMetadata recordAndMetadata, SerializerType dataContentType, String dataSchemaUriBase,
                                        String cloudEventsSchemaName) {
        return new DamengCloudEventsMaker(recordAndMetadata, dataContentType, dataSchemaUriBase, cloudEventsSchemaName);
    }
}
