package org.devlive.connector.dameng.logminer.event;

public enum EventType {

    INSERT(1),
    DELETE(2),
    UPDATE(3),
    DDL(5),
    START(6),
    COMMIT(7),
    SELECT_LOB_LOCATOR(9),
    LOB_WRITE(10),
    LOB_TRIM(11),
    REPLICATION_MARKER(27),
    LOB_ERASE(29),
    MISSING_SCN(34),
    ROLLBACK(36),
    XML_BEGIN(68),
    XML_WRITE(70),
    XML_END(71),
    EXTENDED_STRING_BEGIN(91),
    EXTENDED_STRING_WRITE(92),
    EXTENDED_STRING_END(93),
    UNSUPPORTED(255);

    private static EventType[] types = new EventType[256];

    static {
        for (EventType option : EventType.values()) {
            types[option.getValue()] = option;
        }
    }

    private int value;

    EventType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    /**
     * Resolve an EventType from a numeric event type operation code.
     *
     * @param value the operation code
     * @return the event type, will be {@link #UNSUPPORTED} if the code is not supported.
     */
    public static EventType from(int value) {
        return value < types.length ? types[value] : EventType.UNSUPPORTED;
    }
    
}
