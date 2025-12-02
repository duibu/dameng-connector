/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;
import org.devlive.connector.dameng.DamengConnection;
import org.devlive.connector.dameng.DamengConnectorConfig;
import org.devlive.connector.dameng.DamengDatabaseSchema;
import org.devlive.connector.dameng.DamengStreamingChangeEventSourceMetrics;
import org.devlive.connector.dameng.Scn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * This class contains methods to configure and manage LogMiner utility
 */
public class LogMinerHelper {
    /**
     * Returns a 0-based index offset for the column name in the relational table.
     *
     * @param columnName the column name, should not be {@code null}.
     * @param table the relational table, should not be {@code null}.
     * @return the 0-based index offset for the column name
     */
    public static int getColumnIndexByName(String columnName, Table table) {
        final Column column = table.columnWithName(columnName);
        if (column == null) {
            throw new DebeziumException("No column '" + columnName + "' found in table '" + table.id() + "'");
        }
        // want to return a 0-based index and column positions are 1-based
        return getColumnIndexByName(column);
    }

    public static int getColumnIndexByName(Column column) {
        return column.position() - 1;
    }
}
