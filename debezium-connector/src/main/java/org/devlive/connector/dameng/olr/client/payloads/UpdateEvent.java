/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.olr.client.payloads;

/**
 * Represents an update event.
 *
 * @author Chris Cranford
 */
public class UpdateEvent extends AbstractMutationEvent {
    public UpdateEvent() {
        super(Type.UPDATE);
    }
}
