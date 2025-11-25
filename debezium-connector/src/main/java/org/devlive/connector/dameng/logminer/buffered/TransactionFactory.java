/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.buffered;


/**
 * A factory for creating specific types of {@link Transaction} instances.
 *
 * @author Chris Cranford
 */
public interface TransactionFactory<T extends Transaction> {
    /**
     * Create a transaction from the mined event.
     *
     * @param event the mined event, should not be {@code null}
     * @return the constructed transaction instance, never {@code null}
     */
    T createTransaction(LogMinerEventRow event);
}
