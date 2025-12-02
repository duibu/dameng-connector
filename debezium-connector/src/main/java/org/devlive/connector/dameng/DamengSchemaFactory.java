/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import io.debezium.schema.SchemaFactory;

public class DamengSchemaFactory extends SchemaFactory {

    public DamengSchemaFactory() {
        super();
    }

    private static final DamengSchemaFactory DAMENG_SCHEMA_FACTORY_OBJECT = new DamengSchemaFactory();

    public static DamengSchemaFactory get() {
        return DAMENG_SCHEMA_FACTORY_OBJECT;
    }

}
