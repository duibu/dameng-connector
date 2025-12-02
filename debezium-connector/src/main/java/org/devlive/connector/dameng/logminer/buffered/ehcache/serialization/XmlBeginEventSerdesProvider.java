/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.buffered.ehcache.serialization;


import org.devlive.connector.dameng.logminer.event.XmlBeginEvent;

import java.io.IOException;

/**
 * A specialized implementation of {@link SerdesProvider} for {@link XmlBeginEvent} types.
 *
 * @author Chris Cranford
 */
public class XmlBeginEventSerdesProvider<T extends XmlBeginEvent> extends DmlEventSerdesProvider<T> {
    @Override
    public Class<?> getJavaType() {
        return XmlBeginEvent.class;
    }

    @Override
    public void serialize(XmlBeginEvent event, SerializerOutputStream stream) throws IOException {
        super.serialize(event, stream);

        stream.writeString(event.getColumnName());
    }

    @Override
    public void deserialize(DeserializationContext context, SerializerInputStream stream) throws IOException {
        super.deserialize(context, stream);

        context.addValue(stream.readString());
    }
}
