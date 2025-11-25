package org.devlive.connector.dameng.logminer.buffered.infinispan;

import org.devlive.connector.dameng.Scn;
import org.devlive.connector.dameng.logminer.buffered.AbstractTransaction;
import org.devlive.connector.dameng.logminer.buffered.infinispan.marshalling.VisibleForMarshalling;

import java.time.Instant;

public class InfinispanTransaction extends AbstractTransaction {
    private int numberOfEvents;

    public InfinispanTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, Integer redoThreadId, String clientId) {
        super(transactionId, startScn, changeTime, userName, redoThreadId, clientId);
        start();
    }

    @VisibleForMarshalling
    public InfinispanTransaction(String transactionId, Scn startScn, Instant changeTime, String userName, int numberOfEvents, Integer redoThreadId, String clientId) {
        this(transactionId, startScn, changeTime, userName, redoThreadId, clientId);
        this.numberOfEvents = numberOfEvents;
    }

    @Override
    public int getNumberOfEvents() {
        return numberOfEvents;
    }

    @Override
    public int getNextEventId() {
        return numberOfEvents++;
    }

    @Override
    public void start() {
        numberOfEvents = 0;
    }

    public String getEventId(int index) {
        if (index < 0 || index >= numberOfEvents) {
            throw new IndexOutOfBoundsException("Index " + index + "outside the transaction " + getTransactionId() + " event list bounds");
        }
        return getTransactionId() + "-" + index;
    }

    @Override
    public String toString() {
        return "InfinispanTransaction{" +
                "numberOfEvents=" + numberOfEvents +
                "} " + super.toString();
    }
}
