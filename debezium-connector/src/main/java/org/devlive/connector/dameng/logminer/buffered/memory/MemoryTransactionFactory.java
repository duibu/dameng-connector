/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.buffered.memory;


import org.devlive.connector.dameng.logminer.buffered.TransactionFactory;
import org.devlive.connector.dameng.logminer.event.LogMinerEventRow;

/**
 * Transaction factory implementation for {@link MemoryTransaction}.
 *
 * @author Chris Cranford
 */
public class MemoryTransactionFactory implements TransactionFactory<MemoryTransaction> {
    @Override
    public MemoryTransaction createTransaction(LogMinerEventRow event) {
        return new MemoryTransaction(event.getTransactionId(), event.getScn(), event.getChangeTime(),
                event.getUserName(), event.getThread(), event.getClientId());
    }
}
