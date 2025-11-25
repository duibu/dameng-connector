package org.devlive.connector.dameng.logminer.buffered;

import org.devlive.connector.dameng.Scn;

import java.time.Instant;

public interface Transaction {

    /**
     * Get the transaction identifier
     *
     * @return the transaction unique identifier, never {@code null}
     */
    String getTransactionId();

    /**
     * Get the system change number of when the transaction started
     *
     * @return the system change number, never {@code null}
     */
    Scn getStartScn();

    /**
     * Get the time when the transaction started
     *
     * @return the timestamp of the transaction, never {@code null}
     */
    Instant getChangeTime();

    /**
     * Get the username associated with the transaction
     *
     * @return the username, may be {@code null}
     */
    String getUserName();

    /**
     * Get the client id associated with the transaction
     *
     * @return the client id, may be {@code null}
     */
    String getClientId();

    /**
     * Get the number of events participating in the transaction.
     *
     * @return the number of transaction events
     */
    int getNumberOfEvents();

    /**
     * Return the id of an event based on its index.
     * @param index the index of the event
     * @return the event id
     */
    default String getEventId(int index) {
        if (index < 0 || index >= getNumberOfEvents()) {
            throw new IndexOutOfBoundsException("Index " + index + "outside the transaction " + getTransactionId() + " event list bounds");
        }
        return getTransactionId() + "-" + index;
    }

    /**
     * Helper method to get the next event identifier for the transaction.
     *
     * @return the next event identifier
     */
    int getNextEventId();

    /**
     * Get the redo thread that the transaction participated on.
     *
     * @return the redo thread number
     */
    int getRedoThreadId();

    /**
     * Helper method that resets the event identifier back to {@code 0}.
     * <p>
     * This should be called when a transaction {@code START} event is detected in the event stream.
     * This is required when LOB support is enabled to facilitate the re-mining of existing events.
     */
    void start();

}
