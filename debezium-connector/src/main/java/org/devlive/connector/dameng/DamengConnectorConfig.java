/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.debezium.config.*;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.document.Document;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.Strings;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.devlive.connector.dameng.logminer.HistoryRecorder;
import org.devlive.connector.dameng.logminer.NeverHistoryRecorder;
import org.devlive.connector.dameng.logminer.SqlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Connector configuration for Oracle.
 *
 * @author Gunnar Morling
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP", "MS_SHOULD_BE_FINAL", "NP_NULL_PARAM_DEREF", "NP_BOOLEAN_RETURN_NULL", "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"})
public class DamengConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    protected static final int DEFAULT_PORT = 1528;
    protected static final int DEFAULT_LOG_FILE_QUERY_MAX_RETRIES = 5;

    protected final static int DEFAULT_BATCH_SIZE = 20_000;
    protected final static int DEFAULT_BATCH_INCREMENT_SIZE = 20_000;
    protected final static int MIN_BATCH_SIZE = 1_000;
    protected final static int MAX_BATCH_SIZE = 100_000;

    protected final static int DEFAULT_SCN_GAP_SIZE = 1_000_000;
    protected final static int DEFAULT_SCN_GAP_TIME_INTERVAL = 20_000;

    protected final static int DEFAULT_TRANSACTION_EVENTS_THRESHOLD = 0;

    protected final static int DEFAULT_QUERY_FETCH_SIZE = 10_000;

    protected final static Duration MAX_SLEEP_TIME = Duration.ofMillis(3_000);
    protected final static Duration DEFAULT_SLEEP_TIME = Duration.ofMillis(1_000);
    protected final static Duration MIN_SLEEP_TIME = Duration.ZERO;
    protected final static Duration SLEEP_TIME_INCREMENT = Duration.ofMillis(200);

    protected final static Duration ARCHIVE_LOG_ONLY_POLL_TIME = Duration.ofMillis(10_000);

    protected final static long DEFAULT_RESUME_POSITION_INTERVAL = 10_000L;

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
            .withDefault(DEFAULT_PORT);

    public static final Field HOSTNAME = RelationalDatabaseConnectorConfig.HOSTNAME
            .withNoValidation()
            .withValidation(DamengConnectorConfig::requiredWhenNoUrl);

    public static final Field PDB_NAME = Field.create(DATABASE_CONFIG_PREFIX + "pdb.name")
            .withDisplayName("PDB name")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("Name of the pluggable database when working with a multi-tenant set-up. "
                    + "The CDB name must be given via " + DATABASE_NAME.name() + " in this case.");
    public static final Field XSTREAM_SERVER_NAME = Field.create(ConfigurationNames.DATABASE_CONFIG_PREFIX + "out.server.name")
            .withDisplayName("XStream out server name")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 9))
            .withValidation(DamengConnectorConfig::validateOutServerName)
            .withDescription("Name of the XStream Out server to connect to.");

    public static final Field INTERVAL_HANDLING_MODE = Field.create("interval.handling.mode")
            .withDisplayName("Interval Handling")
            .withEnum(IntervalHandlingMode.class, IntervalHandlingMode.NUMERIC)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 6))
            .withDescription("Specify how INTERVAL columns should be represented in change events, including: "
                    + "'string' represents values as an exact ISO formatted string; "
                    + "'numeric' (default) represents values using the inexact conversion into microseconds");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Select one of the following snapshot options: "
                    + "'always': The connector runs a snapshot every time that it starts. After the snapshot completes, the connector begins to stream changes from the redo logs.; "
                    + "'initial' (default): If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures the current full state of the configured tables. After the snapshot completes, the connector begins to stream changes from the redo logs. "
                    + "'initial_only': The connector performs a snapshot as it does for the 'initial' option, but after the connector completes the snapshot, it stops, and does not stream changes from the redo logs.; "
                    + "'schema_only': If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures only the schema (table structures), but not any table data. After the snapshot completes, the connector begins to stream changes from the redo logs.; "
                    + "'schema_only_recovery': The connector performs a snapshot that captures only the database schema history. The connector then transitions to streaming from the redo logs. Use this setting to restore a corrupted or lost database schema history topic. Do not use if the database schema was modified after the connector stopped.");

    public static final Field SNAPSHOT_LOCKING_MODE = Field.create("snapshot.locking.mode")
            .withDisplayName("Snapshot locking mode")
            .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.SHARED)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 1))
            .withDescription("Controls how the connector holds locks on tables while performing the schema snapshot. The default is 'shared', "
                    + "which means the connector will hold a table lock that prevents exclusive table access for just the initial portion of the snapshot "
                    + "while the database schemas and other metadata are being read. The remaining work in a snapshot involves selecting all rows from "
                    + "each table, and this is done using a flashback query that requires no locks. However, in some cases it may be desirable to avoid "
                    + "locks entirely which can be done by specifying 'none'. This mode is only safe to use if no schema changes are happening while the "
                    + "snapshot is taken.");

    public static final Field CONNECTOR_ADAPTER = Field.create(ConfigurationNames.DATABASE_CONFIG_PREFIX + "connection.adapter")
            .withDisplayName("Connector adapter")
            .withEnum(ConnectorAdapter.class, ConnectorAdapter.LOG_MINER)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 7))
            .withDescription("The adapter to use when capturing changes from the database. "
                    + "Options include: "
                    + "'logminer': (the default) to capture changes using native Oracle LogMiner; "
                    + "'xstream' to capture changes using Oracle XStreams");

    public static final Field LOG_MINING_STRATEGY = Field.create("log.mining.strategy")
            .withDisplayName("Log Mining Strategy")
            .withEnum(LogMiningStrategy.class, LogMiningStrategy.ONLINE_CATALOG)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 8))
            .withValidation(DamengConnectorConfig::validateLogMiningStrategy)
            .withDescription("There are strategies: Online catalog with faster mining but no captured DDL. Another - with data dictionary loaded into REDO LOG files");

    public static final Field SNAPSHOT_ENHANCEMENT_TOKEN = Field.createInternal("snapshot.enhance.predicate.scn")
            .withDisplayName("A string to replace on snapshot predicate enhancement")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 11))
            .withDescription("A token to replace on snapshot predicate template");

    public static final Field LOG_MINING_TRANSACTION_RETENTION_MS = Field.create("log.mining.transaction.retention.ms")
            .withDisplayName("Log Mining long running transaction retention")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(0L)
            .withValidation(Field::isNonNegativeLong)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 19))
            .withDescription("Duration in milliseconds to keep long running transactions in transaction buffer between log mining " +
                    "sessions. By default, all transactions are retained.");

    public static final Field RAC_NODES = Field.create("rac.nodes")
            .withDisplayName("Oracle RAC nodes")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withValidation(DamengConnectorConfig::validateRacNodes)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 11))
            .withDescription("A comma-separated list of RAC node hostnames or ip addresses");

    public static final Field URL = Field.create(ConfigurationNames.DATABASE_CONFIG_PREFIX + "url")
            .withDisplayName("Complete JDBC URL")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(DamengConnectorConfig::requiredWhenNoHostname)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 10))
            .withDescription("Complete JDBC URL as an alternative to specifying hostname, port and database provided "
                    + "as a way to support alternative connection scenarios.");

    public static final Field LOG_MINING_BATCH_SIZE_MIN = Field.create("log.mining.batch.size.min")
            .withDisplayName("Minimum batch size for reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 13))
            .withDefault(MIN_BATCH_SIZE)
            .withDescription(
                    "The minimum SCN interval size that this connector will try to read from redo/archive logs.");

    public static final Field LOG_MINING_BATCH_SIZE_INCREMENT = Field.create("log.mining.batch.size.increment")
            .withDisplayName("Increment/Decrement batch size for reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 12))
            .withDefault(DEFAULT_BATCH_INCREMENT_SIZE)
            .withDescription("Active batch size will be also increased/decreased by this amount for tuning connector throughput when needed.");

    public static final Field LOG_MINING_BATCH_SIZE_DEFAULT = Field.create("log.mining.batch.size.default")
            .withDisplayName("Default batch size for reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 11))
            .withDefault(DEFAULT_BATCH_SIZE)
            .withDescription("The starting SCN interval size that the connector will use for reading data from redo/archive logs.");

    public static final Field LOG_MINING_BATCH_SIZE_MAX = Field.create("log.mining.batch.size.max")
            .withDisplayName("Maximum batch size for reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 14))
            .withDefault(MAX_BATCH_SIZE)
            .withDescription("The maximum SCN interval size that this connector will use when reading from redo/archive logs.");

    public static final Field LOG_MINING_SLEEP_TIME_MIN_MS = Field.create("log.mining.sleep.time.min.ms")
            .withDisplayName("Minimum sleep time in milliseconds when reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 16))
            .withDefault(MIN_SLEEP_TIME.toMillis())
            .withDescription(
                    "The minimum amount of time that the connector will sleep after reading data from redo/archive logs and before starting reading data again. Value is in milliseconds.");

    public static final Field LOG_MINING_SLEEP_TIME_DEFAULT_MS = Field.create("log.mining.sleep.time.default.ms")
            .withDisplayName("Default sleep time in milliseconds when reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 15))
            .withDefault(DEFAULT_SLEEP_TIME.toMillis())
            .withDescription(
                    "The amount of time that the connector will sleep after reading data from redo/archive logs and before starting reading data again. Value is in milliseconds.");

    public static final Field LOG_MINING_SLEEP_TIME_MAX_MS = Field.create("log.mining.sleep.time.max.ms")
            .withDisplayName("Maximum sleep time in milliseconds when reading redo/archive logs.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 17))
            .withDefault(MAX_SLEEP_TIME.toMillis())
            .withDescription(
                    "The maximum amount of time that the connector will sleep after reading data from redo/archive logs and before starting reading data again. Value is in milliseconds.");

    public static final Field LOG_MINING_SLEEP_TIME_INCREMENT_MS = Field.create("log.mining.sleep.time.increment.ms")
            .withDisplayName("The increment in sleep time in milliseconds used to tune auto-sleep behavior.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 18))
            .withDefault(SLEEP_TIME_INCREMENT.toMillis())
            .withDescription(
                    "The maximum amount of time that the connector will use to tune the optimal sleep time when reading data from LogMiner. Value is in milliseconds.");

    public static final Field LOG_MINING_ARCHIVE_LOG_ONLY_MODE = Field.create("log.mining.archive.log.only.mode")
            .withDisplayName("Specifies whether log mining should only target archive logs or both archive and redo logs")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 9))
            .withDefault(false)
            .withDescription("When set to 'false', the default, the connector will mine both archive log and redo logs to emit change events. " +
                    "When set to 'true', the connector will only mine archive logs. There are circumstances where its advantageous to only " +
                    "mine archive logs and accept latency in event emission due to frequent revolving redo logs.");

    public static final Field LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS = Field.create("log.mining.archive.log.only.scn.poll.interval.ms")
            .withDisplayName("The interval in milliseconds to wait between polls when SCN is not yet in the archive logs")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 29))
            .withDefault(ARCHIVE_LOG_ONLY_POLL_TIME.toMillis())
            .withDescription("The interval in milliseconds to wait between polls checking to see if the SCN is in the archive logs.");

    public static final Field LOG_MINING_PATH_DICTIONARY = Field.create("log.mining.path.dictionary")
            .withDisplayName("Defines the dictionary path for the mining session")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(DamengConnectorConfig::validateDictionaryFromFile)
            .withDescription("This is required when using the connector against a read-only database replica.");

    public static final Field LOG_MINING_READONLY_HOSTNAME = Field.create("log.mining.readonly.hostname")
            .withDisplayName("Read-only connector hostname.")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("The hostname the connector will use to connect and perform read-only operations for the the replica.");

    public static final Field LOB_ENABLED = Field.create("lob.enabled")
            .withDisplayName("Specifies whether the connector supports mining LOB fields and operations")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 21))
            .withDefault(false)
            .withDescription("When set to 'false', the default, LOB fields will not be captured nor emitted. When set to 'true', the connector " +
                    "will capture LOB fields and emit changes for those fields like any other column type.");

    public static final Field LOG_MINING_USERNAME_INCLUDE_LIST = Field.create("log.mining.username.include.list")
            .withDisplayName("List of users to include from LogMiner query")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Comma separated list of usernames to include from LogMiner query.");

    public static final Field LOG_MINING_USERNAME_EXCLUDE_LIST = Field.create("log.mining.username.exclude.list")
            .withDisplayName("List of users to exclude from LogMiner query")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 20))
            .withValidation(DamengConnectorConfig::validateUsernameExcludeList)
            .withDescription("Comma separated list of usernames to exclude from LogMiner query.");

    public static final Field ARCHIVE_DESTINATION_NAME = Field.create("archive.destination.name")
            .withDisplayName("Name of the archive log destination to be used for reading archive logs")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 33))
            .withDescription("Sets the specific archive log destination as the source for reading archive logs." +
                    "When not set, the connector will automatically select the first LOCAL and VALID destination.");

    public static final Field ARCHIVE_LOG_HOURS = Field.create("archive.log.hours")
            .withDisplayName("Archive Log Hours")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 32))
            .withDefault(0)
            .withDescription("The number of hours in the past from SYSDATE to mine archive logs. Using 0 mines all available archive logs");

    public static final Field LOG_MINING_BUFFER_TYPE = Field.create("log.mining.buffer.type")
            .withDisplayName("Controls which buffer type implementation to be used")
            .withEnum(LogMiningBufferType.class, LogMiningBufferType.MEMORY)
            .withValidation(DamengConnectorConfig::validateLogMiningBufferType)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 22))
            .withDescription("The buffer type controls how the connector manages buffering transaction data." + System.lineSeparator() +
                    System.lineSeparator() +
                    "memory - Uses the JVM process' heap to buffer all transaction data." + System.lineSeparator() +
                    System.lineSeparator() +
                    "infinispan_embedded - This option uses an embedded Infinispan cache to buffer transaction data and persist it to disk." + System.lineSeparator() +
                    System.lineSeparator() +
                    "infinispan_remote - This option uses a remote Infinispan cluster to buffer transaction data and persist it to disk." + System.lineSeparator() +
                    System.lineSeparator() +
                    "ehcache - Use ehcache in embedded mode to buffer transaction data and persist it to disk.");

    public static final Field LOG_MINING_BUFFER_TRANSACTION_EVENTS_THRESHOLD = Field.create("log.mining.buffer.transaction.events.threshold")
            .withDisplayName("The maximum number of events a transaction can have before being discarded.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_TRANSACTION_EVENTS_THRESHOLD)
            .withValidation(Field::isNonNegativeLong)
            .withDescription("The number of events a transaction can include before the transaction is discarded. " +
                    "This is useful for managing buffer memory and/or space when dealing with very large transactions. " +
                    "Defaults to 0, meaning that no threshold is applied and transactions can have unlimited events.");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_GLOBAL = Field.create("log.mining.buffer.infinispan.cache.global")
            .withDisplayName("Infinispan 'global' cache configuration")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 28))
            .withValidation(DamengConnectorConfig::validateLogMiningInfinispanCacheConfiguration)
            .withDescription("Specifies the XML configuration for the Infinispan 'global' configuration");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS = Field.create("log.mining.buffer.infinispan.cache.transactions")
            .withDisplayName("Infinispan 'transactions' cache configuration")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 24))
            .withValidation(DamengConnectorConfig::validateLogMiningInfinispanCacheConfiguration)
            .withDescription("Specifies the XML configuration for the Infinispan 'transactions' cache");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS = Field.create("log.mining.buffer.infinispan.cache.processed_transactions")
            .withDisplayName("Infinispan 'processed-transactions' cache configuration")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 26))
            .withValidation(DamengConnectorConfig::validateLogMiningInfinispanCacheConfiguration)
            .withDescription("Specifies the XML configuration for the Infinispan 'processed-transactions' cache");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS = Field.create("log.mining.buffer.infinispan.cache.events")
            .withDisplayName("Infinispan 'events' cache configurations")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 25))
            .withValidation(DamengConnectorConfig::validateLogMiningInfinispanCacheConfiguration)
            .withDescription("Specifies the XML configuration for the Infinispan 'events' cache");

    public static final Field LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES = Field.create("log.mining.buffer.infinispan.cache.schema_changes")
            .withDisplayName("Infinispan 'schema-changes' cache configuration")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 27))
            .withValidation(DamengConnectorConfig::validateLogMiningInfinispanCacheConfiguration)
            .withDescription("Specifies the XML configuration for the Infinispan 'schema-changes' cache");

    public static final Field LOG_MINING_BUFFER_DROP_ON_STOP = Field.create("log.mining.buffer.drop.on.stop")
            .withDisplayName("Controls whether the buffer cache is dropped when connector is stopped")
            .withType(Type.BOOLEAN)
            .withDefault(false)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("When set to true the underlying buffer cache is not retained when the connector is stopped. " +
                    "When set to false (the default), the buffer cache is retained across restarts.");

    public static final Field LOG_MINING_SCN_GAP_DETECTION_GAP_SIZE_MIN = Field.create("log.mining.scn.gap.detection.gap.size.min")
            .withDisplayName("SCN gap size used to detect SCN gap")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 30))
            .withDefault(DEFAULT_SCN_GAP_SIZE)
            .withDescription("Used for SCN gap detection, if the difference between current SCN and previous end SCN is " +
                    "bigger than this value, and the time difference of current SCN and previous end SCN is smaller than " +
                    "log.mining.scn.gap.detection.time.interval.max.ms, consider it a SCN gap.");

    public static final Field LOG_MINING_SCN_GAP_DETECTION_TIME_INTERVAL_MAX_MS = Field.create("log.mining.scn.gap.detection.time.interval.max.ms")
            .withDisplayName("Timer interval used to detect SCN gap")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 31))
            .withDefault(DEFAULT_SCN_GAP_TIME_INTERVAL)
            .withDescription("Used for SCN gap detection, if the difference between current SCN and previous end SCN is " +
                    "bigger than log.mining.scn.gap.detection.gap.size.min, and the time difference of current SCN and previous end SCN is smaller than " +
                    " this value, consider it a SCN gap.");

    public static final Field LOG_MINING_LOG_QUERY_MAX_RETRIES = Field.createInternal("log.mining.log.query.max.retries")
            .withDisplayName("Maximum number of retries before failing to locate redo logs")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_LOG_FILE_QUERY_MAX_RETRIES)
            .withValidation(Field::isPositiveInteger)
            .withDescription("The maximum number of log query retries before throwing an exception that logs cannot be found.");

    public static final Field LOG_MINING_LOG_BACKOFF_INITIAL_DELAY_MS = Field.createInternal("log.mining.log.backoff.initial.delay.ms")
            .withDisplayName("Initial delay when logs cannot yet be found (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(TimeUnit.SECONDS.toMillis(1))
            .withValidation(Field::isPositiveInteger)
            .withDescription("The initial delay when trying to query database redo logs, given in milliseconds. Defaults to 1 second (1,000 ms).");

    public static final Field LOG_MINING_LOG_BACKOFF_MAX_DELAY_MS = Field.createInternal("log.mining.log.backoff.max.delay.ms")
            .withDisplayName("Maximum delay when logs cannot yet be found (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(TimeUnit.MINUTES.toMillis(1))
            .withValidation(Field::isPositiveInteger)
            .withDescription("The maximum delay when trying to query database redo logs, given in milliseconds. Defaults to 60 seconds (60,000 ms).");

    public static final Field LOG_MINING_SESSION_MAX_MS = Field.create("log.mining.session.max.ms")
            .withDisplayName("Maximum number of milliseconds of a single LogMiner session")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(TimeUnit.MINUTES.toMillis(0))
            .withValidation(Field::isNonNegativeInteger)
            .withDescription(
                    "The maximum number of milliseconds that a LogMiner session lives for before being restarted. Defaults to 0 (indefinite until a log switch occurs)");

    public static final Field LOG_MINING_RESTART_CONNECTION = Field.create("log.mining.restart.connection")
            .withDisplayName("Restarts Oracle database connection when reaching maximum session time or database log switch")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription("Debezium opens a database connection and keeps that connection open throughout the entire streaming phase. " +
                    "In some situations, this can lead to excessive SGA memory usage. " +
                    "By setting this option to 'true' (the default is 'false'), the connector will close and re-open a database connection " +
                    "after every detected log switch or if the log.mining.session.max.ms has been reached.");

    public static final Field LOG_MINING_TRANSACTION_SNAPSHOT_BOUNDARY_MODE = Field.createInternal("log.mining.transaction.snapshot.boundary.mode")
            .withDisplayName("Transaction snapshot boundary mode")
            .withEnum(TransactionSnapshotBoundaryMode.class, TransactionSnapshotBoundaryMode.SKIP)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Specifies how in-progress transactions are to be handled when resolving the snapshot SCN. " + System.lineSeparator() +
                    "all - Captures in-progress transactions from both V$TRANSACTION and starting a LogMiner session near the snapshot SCN." + System.lineSeparator() +
                    "transaction_view_only - Captures in-progress transactions based on data in V$TRANSACTION only. " +
                    "Recently committed transactions near the flashback query SCN won't be included in the snapshot nor streaming." + System.lineSeparator() +
                    "skip - Skips gathering any in-progress transactions.");

    public static final Field LOG_MINING_QUERY_FILTER_MODE = Field.create("log.mining.query.filter.mode")
            .withDisplayName("Specifies how the filter configuration is applied to the LogMiner database query")
            .withEnum(LogMiningQueryFilterMode.class, LogMiningQueryFilterMode.NONE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specifies how the filter configuration is applied to the LogMiner database query. " + System.lineSeparator() +
                    "none - The query does not apply any schema or table filters, all filtering is at runtime by the connector." + System.lineSeparator() +
                    "in - The query uses SQL in-clause expressions to specify the schema or table filters." + System.lineSeparator() +
                    "regex - The query uses Oracle REGEXP_LIKE expressions to specify the schema or table filters." + System.lineSeparator());

    public static final Field LOG_MINING_READ_ONLY = Field.createInternal("log.mining.read.only")
            .withDisplayName("Runs the connector in read-only mode")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(Boolean.FALSE)
            .withValidation(DamengConnectorConfig::validateLogMiningReadOnly)
            .withDescription("When set to 'true', the connector will not attempt to flush the LGWR buffer to disk, allowing connecting to read-only databases.");

    public static final Field LOG_MINING_FLUSH_TABLE_NAME = Field.create("log.mining.flush.table.name")
            .withDisplayName("Specifies the name of the flush table used by the connector")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault("LOG_MINING_FLUSH")
            .withValidation(DamengConnectorConfig::validateLogMiningFlushTableName)
            .withDescription("The name of the flush table used by the connector, defaults to LOG_MINING_FLUSH.");

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(DamengSourceInfoStructMaker.class.getName());

    public static final Field QUERY_FETCH_SIZE = CommonConnectorConfig.QUERY_FETCH_SIZE
            .withDescription(
                    "The maximum number of records that should be loaded into memory while streaming. A value of '0' uses the default JDBC fetch size, defaults to '2000'.")
            .withDefault(DEFAULT_QUERY_FETCH_SIZE);

    public static final Field LOG_MINING_MAX_SCN_DEVIATION_MS = Field.createInternal("log.mining.max.scn.deviation.ms")
            .withDisplayName("Allows applying a time-based deviation to the max mining scn")
            .withType(Type.LONG)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(0)
            .withValidation(Field::isNonNegativeLong)
            .withDescription("By default, LogMiner will apply no deviation, meaning that the connector can mine up to the CURRENT_SCN. " +
                    "There are situations where this could be problematic if perhaps when asynchronous IO operations are at play. " +
                    "By applying a time-based deviation, for example 3000, the connector will only mine up the SCN that is a result of " +
                    "the formula of TIMESTAMP_TO_SCN(SCN_TO_TIMESTAMP(CURRENT_SCN)-(3000/86400000)). If this SCN is not available, the " +
                    "connector will log a warning and proceed to use the CURRENT_SCN or previously calculated upper SCN regardless. " +
                    "NOTE: This option is internal and should not be used for general use. Using this option will create a net latency " +
                    "on change events increased by the deviation value specified.");

    public static final Field OLR_SOURCE = Field.create("openlogreplicator.source")
            .withDisplayName("The logical source to stream changes from")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withValidation(DamengConnectorConfig::validateRequiredWhenUsingOpenLogReplicator)
            .withDescription("The configured logical source name in the OpenLogReplicator configuration that is to stream changes");

    public static final Field OLR_HOST = Field.create("openlogreplicator.host")
            .withDisplayName("The hostname of the OpenLogReplicator network service")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withValidation(DamengConnectorConfig::validateRequiredWhenUsingOpenLogReplicator)
            .withDescription("The hostname of the OpenLogReplicator network service");

    public static final Field OLR_PORT = Field.create("openlogreplicator.port")
            .withDisplayName("The port of the OpenLogReplicator network service")
            .withType(Type.INT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withValidation(DamengConnectorConfig::validateRequiredWhenUsingOpenLogReplicator)
            .withDescription("The port of the OpenLogReplicator network service");

    public static final Field LOG_MINING_SCHEMA_CHANGES_USERNAME_EXCLUDE_LIST = Field.createInternal("log.mining.schema_changes.username.exclude.list")
            .withDisplayName("Username exclusion list for schema changes")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("A comma-separated list of usernames that schema changes will be skipped for. Defaults to 'SYS,SYSTEM'.")
            .withDefault("SYS,SYSTEM");

    public static final Field LOG_MINING_INCLUDE_REDO_SQL = Field.create("log.mining.include.redo.sql")
            .withDisplayName("Include the transaction log SQL")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("When enabled, the transaction log REDO SQL will be included in the source information block.")
            .withDefault(false)
            .withValidation(DamengConnectorConfig::validateLogMiningIncludeRedoSql);

    public static final Field SNAPSHOT_DATABASE_ERRORS_MAX_RETRIES = Field.create("snapshot.database.errors.max.retries")
            .withDisplayName("The maximum number of retries before snapshot database errors are not retried")
            .withType(Type.INT)
            .withDefault(0)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withValidation(Field::isNonNegativeInteger)
            .withDescription("The number of attempts to retry database errors during snapshots before failing.");

    public static final Field LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG = Field.create("log.mining.buffer.ehcache.global.config")
            .withDisplayName("Defines any global configuration for the Ehcache transaction buffer")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(DamengConnectorConfig::validateEhCacheGlobalConfigField)
            .withDescription("Specifies any Ehcache global configurations such as services or persistence. " +
                    "This cannot include <cache/> nor <default-serializers/> tags as these are managed by Debezium.");

    public static final Field LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG = Field.create("log.mining.buffer.ehcache.transactions.config")
            .withDisplayName("Defines the partial ehcache configuration for the transaction cache")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(DamengConnectorConfig::validateEhcacheConfigFieldRequired)
            .withDescription("Specifies the inner body the Ehcache <cache/> tag for the transaction cache, but " +
                    "should not include the <key-type/> nor the <value-type/> attributes as these are managed by Debezium.");

    public static final Field LOG_MINING_BUFFER_EHCACHE_PROCESSED_TRANSACTIONS_CONFIG = Field.create("log.mining.buffer.ehcache.processedtransactions.config")
            .withDisplayName("Defines the partial ehcache configuration for the processed transaction cache")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(DamengConnectorConfig::validateEhcacheConfigFieldRequired)
            .withDescription("Specifies the inner body the Ehcache <cache/> tag for the processed transaction cache, but " +
                    "should not include the <key-type/> nor the <value-type/> attributes as these are managed by Debezium.");

    public static final Field LOG_MINING_BUFFER_EHCACHE_SCHEMA_CHANGES_CONFIG = Field.create("log.mining.buffer.ehcache.schemachanges.config")
            .withDisplayName("Defines the partial ehcache configuration for the schema changes cache")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(DamengConnectorConfig::validateEhcacheConfigFieldRequired)
            .withDescription("Specifies the inner body the Ehcache <cache/> tag for the schema changes cache, but " +
                    "should not include the <key-type/> nor the <value-type/> attributes as these are managed by Debezium.");

    public static final Field LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG = Field.create("log.mining.buffer.ehcache.events.config")
            .withDisplayName("Defines the partial ehcache configuration for the events cache")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withValidation(DamengConnectorConfig::validateEhcacheConfigFieldRequired)
            .withDescription("Specifies the inner body the Ehcache <cache/> tag for the events cache, but " +
                    "should not include the <key-type/> nor the <value-type/> attributes as these are managed by Debezium.");

    @Deprecated
    public static final Field LOG_MINING_CONTINUOUS_MINE = Field.create("log.mining.continuous.mine")
            .withDisplayName("Should log mining session configured with CONTINUOUS_MINE setting?")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withValidation(Field::isBoolean)
            .withDescription("(Deprecated) if true, CONTINUOUS_MINE option will be added to the log mining session. " +
                    "This will manage log files switches seamlessly.");

    public static final Field OBJECT_ID_CACHE_SIZE = Field.createInternal("object.id.cache.size")
            .withDisplayName("Controls the maximum size of the object ID cache")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withDefault(256)
            .withImportance(Importance.LOW)
            .withValidation(DamengConnectorConfig::validateObjectIdCacheSize)
            .withDescription("The connector maintains a least-recently used cache of database table object ID to name mappings. "
                    + "This controls the maximum capacity of this cache.");

    public static final Field LOG_MINING_SQL_RELAXED_QUOTE_DETECTION = Field.createInternal("log.mining.sql.relaxed.quote.detection")
            .withDisplayName("Controls whether single-quote detection is relaxed")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withDefault(false)
            .withImportance(Importance.LOW)
            .withDescription("When Oracle is configured to use EXTENDED string sizes, there are some use cases where LogMiner will " +
                    "not escape single quotes within a column value, which will lead to value truncation.");

    public static final Field LOG_MINING_CLIENTID_INCLUDE_LIST = Field.create("log.mining.clientid.include.list")
            .withDisplayName("List of client ids to include from LogMiner query")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Comma separated list of client ids to include from LogMiner query.");

    public static final Field LOG_MINING_CLIENTID_EXCLUDE_LIST = Field.create("log.mining.clientid.exclude.list")
            .withDisplayName("List of client ids to exclude from LogMiner query")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withValidation(DamengConnectorConfig::validateClientIdExcludeList)
            .withDescription("Comma separated list of client ids to exclude from LogMiner query.");

    public static final Field LOG_MINING_RESUME_POSITION_INTERVAL_MS = Field.createInternal("log.mining.resume.position.interval.ms")
            .withDisplayName("The interval that the LogMiner unbuffered resume position is updated")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_RESUME_POSITION_INTERVAL)
            .withValidation(Field::isPositiveLong)
            .withDescription("The interval that the resume position is updated");

    public static final Field SIGNAL_DATA_COLLECTION = CommonConnectorConfig.SIGNAL_DATA_COLLECTION
            .withValidation(DamengConnectorConfig::validateSignalDataCollection);

    public static final Field LOG_MINING_BUFFER_MEMORY_LEGACY_TRANSACTION_START = Field.createInternal("log.mining.buffer.memory.legacy.transaction.start")
            .withDisplayName("Use legacy transaction start behavior")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withValidation(DamengConnectorConfig::validateIncludeTransactionStartEvents)
            .withDescription("Controls whether transaction start events are buffered when using the heap/memory buffer type. " +
                    "true: transaction start events are not buffered; " +
                    "false: (the default) transaction start events are buffered");

    public static final Field LEGACY_DECIMAL_HANDLING_STRATEGY = Field.create("legacy.decimal.handling.strategy")
            .withDisplayName("Use legacy decimal handling strategy")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription("Uses the legacy decimal handling behavior before DBZ-7882");

    public static final Field LOG_MINING_USE_CTE_QUERY = Field.createInternal("log.mining.use.cte.query")
            .withDisplayName("Use CTE-based query")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withValidation(DamengConnectorConfig::validateLogMiningUseCteQuery)
            .withDescription("Uses a CTE query to exclude non-relevant transaction markers");

    public static final Field LOG_MINING_REDO_THREAD_SCN_ADJUSTMENT = Field.createInternal("log.mining.redo.thread.scn.adjustment")
            .withDisplayName("SCN adjustment value")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(1)
            .withDescription("Adjusts the LAST_REDO_SCN from V$THREAD by this value.");

    public static final Field LOG_MINING_HASH_AREA_SIZE = Field.createInternal("log.mining.hash.area.size")
            .withDisplayName("Hash Area Size")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(0)
            .withValidation(Field::isNonNegativeLong)
            .withDescription("Specifies the maximum memory in bytes the LogMiner session can use for performing SQL join operations. " +
                    "Setting this to 0 (the default) uses the database's default HASH_AREA_SIZE.");

    public static final Field LOG_MINING_SORT_AREA_SIZE = Field.createInternal("log.mining.sort.area.size")
            .withDisplayName("Sort Area Size")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(0)
            .withValidation(Field::isNonNegativeLong)
            .withDescription("Specifies the maximum memory in bytes the LogMiner session can use for performing SQL sort operations. " +
                    "Setting this to 0 (the default) uses the database's default SORT_AREA_SIZE.");
    
    @Deprecated
    public static final Field TABLENAME_CASE_INSENSITIVE = Field.create("database.tablename.case.insensitive")
            .withDisplayName("Case insensitive table names")
            .withType(Type.BOOLEAN)
            .withDefault(false)
            .withImportance(Importance.LOW)
            .withDescription("Deprecated: Case insensitive table names; set to 'true' for Oracle 11g, 'false' (default) otherwise.");
    public static final Field ORACLE_VERSION = Field.createInternal("database.oracle.version")
            .withDisplayName("Oracle version, 11 or 12+")
            .withType(Type.STRING)
            .withImportance(Importance.LOW)
            .withDescription("Deprecated: For default Oracle 12+, use default pos_version value v2, for Oracle 11, use pos_version value v1.");
    //    public static final Field SERVER_NAME = RelationalDatabaseConnectorConfig.SERVER_NAME
//            .withValidation(CommonConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName);
    
    // this option could be true up to Oracle 18c version. Starting from Oracle 19c this option cannot be true todo should we do it?
    public static final Field CONTINUOUS_MINE = Field.create("log.mining.continuous.mine")
            .withDisplayName("Should log mining session configured with CONTINUOUS_MINE setting?")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withValidation(Field::isBoolean)
            .withDescription("If true, CONTINUOUS_MINE option will be added to the log mining session. This will manage log files switches seamlessly.");
   
    public static final Field LOG_MINING_HISTORY_RECORDER_CLASS = Field.create("log.mining.history.recorder.class")
            .withDisplayName("Log Mining History Recorder Class")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withInvisibleRecommender()
            .withDescription("Allows connector deployment to capture log mining results");
    public static final Field LOG_MINING_HISTORY_RETENTION = Field.create("database.history.retention.hours")
            .withDisplayName("Log Mining history retention")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(0)
            .withDescription("Hours to keep Log Mining history.  By default, no history is retained.");
    public static final Field LOG_MINING_TRANSACTION_RETENTION = Field.create("log.mining.transaction.retention.hours")
            .withDisplayName("Log Mining long running transaction retention")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(0)
            .withValidation(Field::isNonNegativeInteger)
            .withDescription("Hours to keep long running transactions in transaction buffer between log mining sessions.  By default, all transactions are retained.");
    
    public static final Field RAC_SYSTEM = Field.create("database.rac")
            .withDisplayName("Oracle RAC")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDefault(false)
            .withDescription("Flag to if it is RAC system");
    
    public static final Field LOG_MINING_DML_PARSER = Field.createInternal("log.mining.dml.parser")
            .withDisplayName("Log Mining DML parser implementation")
            .withEnum(LogMiningDmlParser.class, LogMiningDmlParser.FAST)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The parser implementation to use when parsing DML operations:" +
                    "'legacy': the legacy parser implementation based on JSqlParser; " +
                    "'fast': the robust parser implementation that is streamlined specifically for LogMiner redo format");
    public static final Field LOG_MINING_ARCHIVE_LOG_HOURS = Field.create("log.mining.archive.log.hours")
            .withDisplayName("Log Mining Archive Log Hours")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(0)
            .withDescription("The number of hours in the past from SYSDATE to mine archive logs.  Using 0 mines all available archive logs");
    public static final List<String> EXCLUDED_SCHEMAS = Collections.unmodifiableList(Arrays.asList("appqossys", "audsys",
            "ctxsys", "dvsys", "dbsfwuser", "dbsnmp", "gsmadmin_internal", "lbacsys", "mdsys", "ojvmsys", "olapsys",
            "orddata", "ordsys", "outln", "sys", "system", "wmsys", "xdb"));
    
    protected static final int DEFAULT_VIEW_FETCH_SIZE = 10_000;
    public static final Field LOG_MINING_VIEW_FETCH_SIZE = Field.create("log.mining.view.fetch.size")
            .withDisplayName("Number of content records that will be fetched.")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_VIEW_FETCH_SIZE)
            .withDescription("The number of content records that will be fetched from the LogMiner content view.");
    
    public static final Field AUTO_COMMIT_TIMEOUT = Field.create("debezium.source.transaction.auto.commit.timeout.ms")
            .withDisplayName("Transaction auto-commit timeout in milliseconds")
            .withType(Type.LONG)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDefault(1000L)
            .withDescription("The time in milliseconds after which an uncommitted transaction will be auto-committed.");

    private static final Logger LOGGER = LoggerFactory.getLogger(DamengConnectorConfig.class);
    private final String databaseName;
    private final String pdbName;
    private final String xoutServerName;
    private final SnapshotMode snapshotMode;
    private final Boolean tablenameCaseInsensitive;
    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("Dameng")
            .excluding(
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_EXCLUDE_LIST,
                    RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
                    CommonConnectorConfig.QUERY_FETCH_SIZE,
                    CommonConnectorConfig.SIGNAL_DATA_COLLECTION)
            .type(
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    DATABASE_NAME,
                    PDB_NAME,
                    XSTREAM_SERVER_NAME,
                    SNAPSHOT_MODE,
                    CONNECTOR_ADAPTER,
                    LOG_MINING_STRATEGY,
                    URL,
                    TABLENAME_CASE_INSENSITIVE,
                    ORACLE_VERSION)
            .connector(
                    QUERY_FETCH_SIZE,
                    SNAPSHOT_ENHANCEMENT_TOKEN,
                    RAC_SYSTEM,
                    RAC_NODES,
                    LOG_MINING_HISTORY_RECORDER_CLASS,
                    LOG_MINING_HISTORY_RETENTION,
                    LOG_MINING_ARCHIVE_LOG_HOURS,
                    LOG_MINING_BATCH_SIZE_DEFAULT,
                    LOG_MINING_BATCH_SIZE_MIN,
                    LOG_MINING_BATCH_SIZE_MAX,
                    LOG_MINING_SLEEP_TIME_DEFAULT_MS,
                    LOG_MINING_SLEEP_TIME_MIN_MS,
                    LOG_MINING_SLEEP_TIME_MAX_MS,
                    LOG_MINING_SLEEP_TIME_INCREMENT_MS,
                    LOG_MINING_TRANSACTION_RETENTION,
                    LOG_MINING_DML_PARSER,
                    AUTO_COMMIT_TIMEOUT
            )
            .create();
    private final String oracleVersion;
    private final HistoryRecorder logMiningHistoryRecorder;
    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALLFIELDS = Field.setOf(CONFIG_DEFINITION.all());
    private final Configuration jdbcConfig;
    private final ConnectorAdapter connectorAdapter;
    private final String snapshotEnhancementToken;
    // LogMiner options
    private final LogMiningStrategy logMiningStrategy;
    private final long logMiningHistoryRetentionHours;
    private final Set<String> racNodes;
    private final boolean logMiningContinuousMine;
    private final Duration logMiningArchiveLogRetention;
    private final int logMiningBatchSizeMin;
    private final int logMiningBatchSizeMax;
    private final int logMiningBatchSizeDefault;
    private final int logMiningViewFetchSize;
    private final Duration logMiningSleepTimeMin;
    private final Duration logMiningSleepTimeMax;
    private final Duration logMiningSleepTimeDefault;
    private final Duration logMiningSleepTimeIncrement;
    private final Duration logMiningTransactionRetention;
    private final Long autoCommitTimeout;
    private final LogMiningDmlParser dmlParser;

    public DamengConnectorConfig(Configuration config) {
        super(
                DamengConnector.class,
                config,
                new SystemTablesPredicate(config),
                x -> x.schema() + "." + x.table(),
                true,
                DEFAULT_QUERY_FETCH_SIZE,
                ColumnFilterMode.SCHEMA,
                false
        );

        this.databaseName = toUpperCase(config.getString(DATABASE_NAME));
        this.pdbName = toUpperCase(config.getString(PDB_NAME));
        this.xoutServerName = config.getString(XSTREAM_SERVER_NAME);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.tablenameCaseInsensitive = resolveTableNameCaseInsensitivity(config);
        this.oracleVersion = config.getString(ORACLE_VERSION);
        this.logMiningHistoryRecorder = resolveLogMiningHistoryRecorder(config);
        this.jdbcConfig = config.subset(DATABASE_CONFIG_PREFIX, true);
        this.snapshotEnhancementToken = config.getString(SNAPSHOT_ENHANCEMENT_TOKEN);

        // LogMiner
        this.connectorAdapter = ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER));
        this.logMiningStrategy = LogMiningStrategy.parse(config.getString(LOG_MINING_STRATEGY));
        this.logMiningHistoryRetentionHours = config.getLong(LOG_MINING_HISTORY_RETENTION);
        this.racNodes = Strings.setOf(config.getString(RAC_NODES), String::new);
        this.logMiningContinuousMine = config.getBoolean(CONTINUOUS_MINE);
        this.logMiningArchiveLogRetention = Duration.ofHours(config.getLong(LOG_MINING_ARCHIVE_LOG_HOURS));
        this.logMiningBatchSizeMin = config.getInteger(LOG_MINING_BATCH_SIZE_MIN);
        this.logMiningBatchSizeMax = config.getInteger(LOG_MINING_BATCH_SIZE_MAX);
        this.logMiningBatchSizeDefault = config.getInteger(LOG_MINING_BATCH_SIZE_DEFAULT);
        this.logMiningViewFetchSize = config.getInteger(LOG_MINING_VIEW_FETCH_SIZE);
        this.logMiningSleepTimeMin = Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_MIN_MS));
        this.logMiningSleepTimeMax = Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_MAX_MS));
        this.logMiningSleepTimeDefault = Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_DEFAULT_MS));
        this.logMiningSleepTimeIncrement = Duration.ofMillis(config.getInteger(LOG_MINING_SLEEP_TIME_INCREMENT_MS));
        this.logMiningTransactionRetention = Duration.ofHours(config.getInteger(LOG_MINING_TRANSACTION_RETENTION));
        this.dmlParser = LogMiningDmlParser.parse(config.getString(LOG_MINING_DML_PARSER));
        this.autoCommitTimeout = config.getLong(AUTO_COMMIT_TIMEOUT);
    }

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    private static String toUpperCase(String property) {
        return property == null ? null : property.toUpperCase();
    }

    private static HistoryRecorder resolveLogMiningHistoryRecorder(Configuration config) {
        if (!config.hasKey(LOG_MINING_HISTORY_RECORDER_CLASS.name())) {
            return new NeverHistoryRecorder();
        }
        return config.getInstance(LOG_MINING_HISTORY_RECORDER_CLASS, HistoryRecorder.class);
    }

    public static int validateOutServerName(Configuration config, Field field, ValidationOutput problems) {
        if (ConnectorAdapter.XSTREAM.equals(ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER)))) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    public static int requiredWhenNoUrl(Configuration config, Field field, ValidationOutput problems) {
        // Validates that the field is required but only when an URL field is not present
        if (config.getString(URL) == null) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    public static int requiredWhenNoHostname(Configuration config, Field field, ValidationOutput problems) {
        // Validates that the field is required but only when an URL field is not present
        if (config.getString(HOSTNAME) == null) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    private static Boolean resolveTableNameCaseInsensitivity(Configuration config) {
        if (config.hasKey(TABLENAME_CASE_INSENSITIVE.name())) {
            LOGGER.warn("The option '{}' is deprecated and will be removed in the future.", TABLENAME_CASE_INSENSITIVE.name());
            return config.getBoolean(TABLENAME_CASE_INSENSITIVE);
        }
        return null;
    }

    public static int validateRacNodes(Configuration config, Field field, ValidationOutput problems) {
        if (isBufferedLogMiner(config)) {
            // If no "database.port" is specified, guarantee that "rac.nodes" (if not empty) specifies the
            // port designation for each comma-delimited value.
            final boolean portProvided = config.hasKey(PORT.name());
            if (!portProvided) {
                int errors = 0;
                final Set<String> racNodes = Strings.setOf(config.getString(RAC_NODES), String::new);
                for (String racNode : racNodes) {
                    String[] parts = racNode.split(":");
                    if (parts.length == 1) {
                        problems.accept(field, racNode, "Must be specified as 'ip/hostname:port' since no 'database.port' is provided");
                        errors++;
                    }
                }
                return errors;
            }
        }
        return 0;
    }

    public static int validateDictionaryFromFile(Configuration config, Field field, ValidationOutput problems) {
        // Validates that the field is required but only when the LogMiner strategy is set to DICTIONARY_FROM_FILE
        if (LogMiningStrategy.DICTIONARY_FROM_FILE.equals(LogMiningStrategy.parse(config.getString(LOG_MINING_STRATEGY)))) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    private static int validateLogMiningBufferType(Configuration config, Field field, ValidationOutput problems) {
        if (isBufferedLogMiner(config)) {
            final String bufferTypeName = config.getString(LOG_MINING_BUFFER_TYPE);
            final LogMiningBufferType bufferType = LogMiningBufferType.parse(bufferTypeName);

            if (bufferType == null) {
                if (!Strings.isNullOrBlank(bufferTypeName)) {
                    LOGGER.error("The option '{}' was specified with an invalid configuration value '{}'",
                            LOG_MINING_BUFFER_TYPE.name(),
                            bufferTypeName);
                }
                return 1;
            } else if (LogMiningBufferType.INFINISPAN_REMOTE.equals(bufferType)) {
                // Must supply the Hotrod server list property as a minimum when using Infinispan cluster mode
                final String serverList = config.getString(RemoteInfinispanCacheProvider.HOTROD_SERVER_LIST);
                if (Strings.isNullOrEmpty(serverList)) {
                    LOGGER.error("The option '{}' must be supplied when using the buffer type '{}'",
                            RemoteInfinispanCacheProvider.HOTROD_SERVER_LIST,
                            bufferType.name());
                    return 1;
                }
            }
        }
        return 0;
    }

    public static int validateLogMiningInfinispanCacheConfiguration(Configuration config, Field field, ValidationOutput problems) {
        if (isBufferedLogMiner(config)) {
            if (LogMiningBufferType.parseWithDefaultFallback(config.getString(LOG_MINING_BUFFER_TYPE)).isInfinispan()) {
                return Field.isRequired(config, field, problems);
            }
        }
        return 0;
    }

    public static int validateLogMiningReadOnly(Configuration config, Field field, ValidationOutput problems) {
        if (isBufferedLogMiner(config)) {
            if (config.getBoolean(LOG_MINING_READ_ONLY)) {
                LOGGER.warn("When using '{}', the LogMiner tablespace requires write access for the Oracle background LogMiner process; however, " +
                        "the connector itself will not perform any write operations against the database.", LOG_MINING_READ_ONLY.name());
                final Set<String> racNodes = Strings.setOf(config.getString(RAC_NODES), String::new);
                if (!racNodes.isEmpty()) {
                    LOGGER.warn("The property '{}' is set, but is ignored due to using read-only mode.", RAC_NODES.name());
                }
            }
        }
        return 0;
    }

    public static int validateLogMiningFlushTableName(Configuration config, Field field, ValidationOutput problems) {
        if (isBufferedLogMiner(config) && !config.getBoolean(LOG_MINING_READ_ONLY)) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    public static int validateUsernameExcludeList(Configuration config, Field field, ValidationOutput problems) {
        if (isLogMiner(config)) {
            final String includeList = config.getString(LOG_MINING_USERNAME_INCLUDE_LIST);
            final String excludeList = config.getString(LOG_MINING_USERNAME_EXCLUDE_LIST);

            if (includeList != null && excludeList != null) {
                problems.accept(LOG_MINING_USERNAME_EXCLUDE_LIST, excludeList,
                        String.format("\"%s\" is already specified", LOG_MINING_USERNAME_INCLUDE_LIST.name()));
                return 1;
            }
        }
        return 0;
    }

    public static int validateRequiredWhenUsingOpenLogReplicator(Configuration config, Field field, ValidationOutput problems) {
        if (ConnectorAdapter.OLR.equals(ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER)))) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    public static int validateLogMiningIncludeRedoSql(Configuration config, Field field, ValidationOutput problems) {
        if (config.getBoolean(field) && isLogMiner(config) && config.getBoolean(LOB_ENABLED)) {
            problems.accept(field, config.getBoolean(field), String.format(
                    "The configuration property '%s' cannot be enabled when '%s' is set to true.",
                    field.name(), LOB_ENABLED.name()));
            return 1;
        }
        return 0;
    }

    public static int validateLogMiningStrategy(Configuration config, Field field, ValidationOutput problems) {
        if (isLogMiner(config) && config.getBoolean(LOB_ENABLED)) {
            // When LOB is enabled, the combination is not valid with the hybrid strategy.
            // This is because we currently are not capable of decoding all LOB-based operations in
            // the LogMiner event stream to support CLOB, NCLOB, BLOB, XML, and JSON just yet.
            // This is an ongoing, work-in-progress strategy.
            final String strategy = config.getString(LOG_MINING_STRATEGY);
            if (LogMiningStrategy.HYBRID.equals(LogMiningStrategy.parse(strategy))) {
                problems.accept(LOG_MINING_STRATEGY, strategy,
                        String.format("The hybrid mining strategy is not compatible when enabling '%s'. " +
                                        "Please use a different '%s' or do not enable '%s'.",
                                LOB_ENABLED.name(), LOG_MINING_STRATEGY.name(), LOB_ENABLED.name()));
                return 1;
            }
        }
        return 0;
    }

    public static int validateEhCacheGlobalConfigField(Configuration config, Field field, ValidationOutput problems) {
        if (isBufferedLogMiner(config)) {
            if (LogMiningBufferType.parseWithDefaultFallback(config.getString(LOG_MINING_BUFFER_TYPE)).isEhcache()) {
                // The string cannot include any `<cache ` or `<default-serializers` tags.
                final String globalConfig = config.getString(LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG, "").toLowerCase();
                if (!Strings.isNullOrEmpty(globalConfig)) {
                    if (globalConfig.contains("<cache") || globalConfig.contains("<default-serializers")) {
                        problems.accept(LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG, globalConfig,
                                "The ehcache global configuration should not contain a <cache/> or <default-serializers/> section");
                        return 1;
                    }
                }
            }
        }
        return 0;
    }

    public static int validateEhcacheConfigFieldRequired(Configuration config, Field field, ValidationOutput problems) {
        if (isBufferedLogMiner(config)) {
            if (LogMiningBufferType.parseWithDefaultFallback(config.getString(LOG_MINING_BUFFER_TYPE)).isEhcache()) {
                return Field.isRequired(config, field, problems);
            }
        }
        return 0;
    }

    public static int validateObjectIdCacheSize(Configuration config, Field field, ValidationOutput problems) {
        if (isLogMiner(config)) {
            int result = Field.isRequired(config, field, problems);
            if (result != 0) {
                return result;
            }
            return Field.isPositiveInteger(config, field, problems);
        }
        return 0;
    }

    public static int validateClientIdExcludeList(Configuration config, Field field, ValidationOutput problems) {
        if (isLogMiner(config)) {
            final String includeList = config.getString(LOG_MINING_CLIENTID_INCLUDE_LIST);
            final String excludeList = config.getString(LOG_MINING_CLIENTID_EXCLUDE_LIST);
            if (includeList != null && excludeList != null) {
                problems.accept(LOG_MINING_CLIENTID_EXCLUDE_LIST, excludeList,
                        String.format("\"%s\": is already specified", LOG_MINING_CLIENTID_INCLUDE_LIST.name()));
                return 1;
            }
        }
        return 0;
    }

    public static int validateSignalDataCollection(Configuration config, Field field, ValidationOutput problems) {
        final String signalDataCollection = config.getString(SIGNAL_DATA_COLLECTION);
        if (!Strings.isNullOrEmpty(signalDataCollection)) {
            final TableId tableId = TableId.parse(signalDataCollection);
            if (Strings.isNullOrEmpty(tableId.catalog())
                    || Strings.isNullOrEmpty(tableId.schema())
                    || Strings.isNullOrEmpty(tableId.table())) {
                problems.accept(SIGNAL_DATA_COLLECTION, signalDataCollection,
                        "Please specify the signal data collection as '<database>.<schema>.<table>'.");
                return 1;
            }
        }
        return 0;
    }

    public static int validateIncludeTransactionStartEvents(Configuration config, Field field, ValidationOutput problems) {
        final boolean includeTransactionStarts = config.getBoolean(LOG_MINING_BUFFER_MEMORY_LEGACY_TRANSACTION_START);
        if (includeTransactionStarts && isBufferedLogMiner(config)) {
            final LogMiningBufferType bufferType = LogMiningBufferType.parse(config.getString(LOG_MINING_BUFFER_TYPE));
            if (!LogMiningBufferType.MEMORY.equals(bufferType)) {
                LOGGER.warn("'{}' only applies to buffered LogMiner with buffer type 'memory', setting will be ignored.",
                        LOG_MINING_BUFFER_MEMORY_LEGACY_TRANSACTION_START.name());
            }
        }
        return 0;
    }

    public static int validateLogMiningUseCteQuery(Configuration config, Field field, ValidationOutput problems) {
        if (config.getBoolean(LOG_MINING_USE_CTE_QUERY)) {
            // When using the CTE, the LogMiningQueryFilterMode must be set
            final String queryFilterMode = config.getString(LOG_MINING_QUERY_FILTER_MODE);
            final LogMiningQueryFilterMode filterMode = LogMiningQueryFilterMode.parse(queryFilterMode);
            if (filterMode == null || filterMode == LogMiningQueryFilterMode.NONE) {
                problems.accept(LOG_MINING_USE_CTE_QUERY, true,
                        "To use CTE query mode, a value must be specified for `log.mining.query.filter.mode`");
                return 1;
            }
        }
        return 0;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getPdbName() {
        return pdbName;
    }

    public String getCatalogName() {
        return pdbName != null ? pdbName : databaseName;
    }

    public String getXoutServerName() {
        return xoutServerName;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    /**
     * Returns whether table name case is insensitive or not.  The method may return {@code null}
     * which indicates the connector configuration does not specify a value and should therefore
     * be resolved by the {@link DamengConnection}.
     *
     * @return whether table case is insensitive, may be {@code null}.
     */
    public Optional<Boolean> getTablenameCaseInsensitive() {
        return Optional.ofNullable(tablenameCaseInsensitive);
    }

    public String getOracleVersion() {
        return oracleVersion;
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                Scn recordedScn;
                Scn desiredScn;
                if (getAdapter() == ConnectorAdapter.XSTREAM) {
                    return false;
                } else {
                    recordedScn = resolveScn(recorded);
                    desiredScn = resolveScn(desired);
                    return recordedScn.compareTo(desiredScn) < 1;
                }
            }

            private Scn resolveScn(Document document) {
                // prioritize reading scn as string and if not found, fallback to long data types
                final String scn = document.getString(SourceInfo.SCN_KEY);
                if (scn == null) {
                    Long scnValue = document.getLong(SourceInfo.SCN_KEY);
                    Scn.valueOf(scnValue == null ? 0 : scnValue);
                }
                return Scn.valueOf(scn);
            }
        };
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return new DamengSourceInfoStructMaker(Module.name(), Module.version(), this);
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    /**
     * @return connection adapter
     */
    public ConnectorAdapter getAdapter() {
        return connectorAdapter;
    }

    /**
     * @return Log Mining strategy
     */
    public LogMiningStrategy getLogMiningStrategy() {
        return logMiningStrategy;
    }

    /**
     * @return whether log mining history is recorded
     */
    public Boolean isLogMiningHistoryRecorded() {
        return logMiningHistoryRetentionHours > 0;
    }

    /**
     * @return the log mining history recorder implementation, may be null
     */
    public HistoryRecorder getLogMiningHistoryRecorder() {
        return logMiningHistoryRecorder;
    }

    /**
     * @return the number of hours log mining history is retained if history is recorded
     */
    public long getLogMinerHistoryRetentionHours() {
        return logMiningHistoryRetentionHours;
    }

    /**
     * @return whether Oracle is using RAC
     */
    public Boolean isRacSystem() {
        return !racNodes.isEmpty();
    }

    /**
     * @return set of node hosts or ip addresses used in Oracle RAC
     */
    public Set<String> getRacNodes() {
        return racNodes;
    }

    /**
     * @return String token to replace
     */
    public String getTokenToReplaceInSnapshotPredicate() {
        return snapshotEnhancementToken;
    }

    /**
     * @return whether continuous log mining is enabled
     */
    public boolean isContinuousMining() {
        return logMiningContinuousMine;
    }

    /**
     * @return the duration that archive logs are scanned for log mining
     */
    public Duration getLogMiningArchiveLogRetention() {
        return logMiningArchiveLogRetention;
    }

    /**
     * @return int The minimum SCN interval used when mining redo/archive logs
     */
    public int getLogMiningBatchSizeMin() {
        return logMiningBatchSizeMin;
    }

    /**
     * @return int Number of actual records that will be fetched from the log mining contents view
     */
    public int getLogMiningViewFetchSize() {
        return logMiningViewFetchSize;
    }

    /**
     * @return int The maximum SCN interval used when mining redo/archive logs
     */
    public int getLogMiningBatchSizeMax() {
        return logMiningBatchSizeMax;
    }

    /**
     * @return int The default SCN interval used when mining redo/archive logs
     */
    public int getLogMiningBatchSizeDefault() {
        return logMiningBatchSizeDefault;
    }

    /**
     * @return int The minimum sleep time used when mining redo/archive logs
     */
    public Duration getLogMiningSleepTimeMin() {
        return logMiningSleepTimeMin;
    }

    /**
     * @return int The maximum sleep time used when mining redo/archive logs
     */
    public Duration getLogMiningSleepTimeMax() {
        return logMiningSleepTimeMax;
    }

    /**
     * @return int The default sleep time used when mining redo/archive logs
     */
    public Duration getLogMiningSleepTimeDefault() {
        return logMiningSleepTimeDefault;
    }

    /**
     * @return int The increment in sleep time when doing auto-tuning while mining redo/archive logs
     */
    public Duration getLogMiningSleepTimeIncrement() {
        return logMiningSleepTimeIncrement;
    }

    /**
     * @return the duration for which long running transactions are permitted in the transaction buffer between log switches
     */
    public Duration getLogMiningTransactionRetention() {
        return logMiningTransactionRetention;
    }

    public long getAutoCommitTimeoutMs() {
        return this.autoCommitTimeout;
    }

    /**
     * @return the log mining parser implementation to be used
     */
    public LogMiningDmlParser getLogMiningDmlParser() {
        return dmlParser;
    }

    public Configuration jdbcConfig() {
        return jdbcConfig;
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    private static boolean isBufferedLogMiner(Configuration config) {
        return ConnectorAdapter.LOG_MINER.equals(ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER)));
    }

    public enum IntervalHandlingMode implements EnumeratedValue {

        /**
         * Represents interval as inexact microseconds count
         */
        NUMERIC("numeric"),

        /**
         * Represents interval as ISO 8601 time interval
         */
        STRING("string");

        private final String value;

        IntervalHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Convert mode name into the logical value
         *
         * @param value the configuration property value ; may not be null
         * @return the matching option, or null if the match is not found
         */
        public static IntervalHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (IntervalHandlingMode option : IntervalHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Convert mode name into the logical value
         *
         * @param value        the configuration property value ; may not be null
         * @param defaultValue the default value ; may be null
         * @return the matching option or null if the match is not found and non-null default is invalid
         */
        public static IntervalHandlingMode parse(String value, String defaultValue) {
            IntervalHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public enum SnapshotLockingMode implements EnumeratedValue {
        /**
         * This mode will allow concurrent access to the table during the snapshot but prevents any
         * session from acquiring any table-level exclusive lock.
         */
        SHARED("shared"),

        /**
         * This mode will avoid using ANY table locks during the snapshot process.
         * This mode should be used carefully only when no schema changes are to occur.
         */
        NONE("none"),

        /**
         * Inject a custom mode, which allows for more control over snapshot locking.
         */
        CUSTOM("custom");

        private final String value;

        SnapshotLockingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean usesLocking() {
            return !value.equals(NONE.value);
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be {@code null}
         * @return the matching option, or null if no match is found
         */
        public static SnapshotLockingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotLockingMode option : SnapshotLockingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value        the configuration property value; may not be {@code null}
         * @param defaultValue the default value; may be {@code null}
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotLockingMode parse(String value, String defaultValue) {
            SnapshotLockingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public enum LogMiningBufferType implements EnumeratedValue {
        MEMORY("memory"),
        INFINISPAN_EMBEDDED("infinispan_embedded"),
        INFINISPAN_REMOTE("infinispan_remote"),
        EHCACHE("ehcache");

        private final String value;

        LogMiningBufferType(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean isInfinispan() {
            return INFINISPAN_EMBEDDED.equals(this) || INFINISPAN_REMOTE.equals(this);
        }

        public boolean isInfinispanEmbedded() {
            return INFINISPAN_EMBEDDED.equals(this);
        }

        public boolean isEhcache() {
            return EHCACHE.equals(this);
        }

        public static LogMiningBufferType parse(String value) {
            if (value == null) {
                return null;
            }

            for (LogMiningBufferType option : LogMiningBufferType.values()) {
                if (option.getValue().equalsIgnoreCase(value.trim())) {
                    return option;
                }
            }

            return null;
        }

        public static LogMiningBufferType parseWithDefaultFallback(String value) {
            return parseOrDefault(value, (String) LOG_MINING_BUFFER_TYPE.defaultValue());
        }

        private static LogMiningBufferType parseOrDefault(String value, String defaultValue) {
            LogMiningBufferType bufferType = parse(value);

            if (bufferType == null && defaultValue != null) {
                return parse(defaultValue);
            }

            return bufferType;
        }

    }

    public enum LogMiningQueryFilterMode implements EnumeratedValue {
        /**
         * This filter mode does not add any predicates to the LogMiner query, all filtering of
         * change data is done at runtime in the connector's Java code. This is the default
         * mode.
         */
        NONE("none"),

        /**
         * This filter mode adds predicates to the LogMiner query, using standard SQL in-clause
         * semantics. This mode expects that the include/exclude connector properties specify
         * schemas and tables without regular expressions.
         *
         * This option may be the best performing option when there is substantially more data in
         * the redo logs compared to the data wanting to be captured at the trade-off that the
         * connector configuration is a bit more verbose with include/exclude filters.
         */
        IN("in"),

        /**
         * This filter mode adds predicates to the LogMiner query, using the Oracle REGEXP_LIKE
         * operator. This mode supports the include/exclude connector properties specifying
         * regular expressions.
         *
         * For the best performance, it's generally a good idea to limit the number of REGEXP_LIKE
         * operators in the query as it's treated similar to the LIKE operator which often does
         * not perform well on large data sets. The number of REGEXP_LIKE operators can be reduced
         * by specifying complex regular expressions where a single expression can potentially
         * match multiple schemas or tables.
         */
        REGEX("regex");

        private final String value;

        LogMiningQueryFilterMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static LogMiningQueryFilterMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (LogMiningQueryFilterMode mode : LogMiningQueryFilterMode.values()) {
                if (mode.getValue().equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            return null;
        }
    }

    public enum TransactionSnapshotBoundaryMode implements EnumeratedValue {
        /**
         * Specifies that the in-progress transaction support at the snapshot boundary should be
         * skipped and that only transactions committed prior to the snapshot SCN and those that
         * are started after the snapshot SCN will be captured.
         */
        SKIP("skip"),

        /**
         * Specifies that in-progress transactions that are available in the {@code V$TRANSACTION}
         * table will be captured and emitted when streaming begins. If a transaction is not in
         * this view, and its changes were not captured by Oracle Flashback query based on the
         * snapshot SCN, that transaction will not be captured.
         */
        TRANSACTION_VIEW_ONLY("transaction_view_only"),

        /**
         * Specifies that in-progress transactions identified in the {@code V$TRANSACTION} table as
         * well as any in-progress transactions as of the current SCN that may have been committed
         * immediately prior to or at the snapshot SCN will be captured. This is done by starting a
         * special LogMiner session to gather these transactions prior to starting the snapshot.
         */
        ALL("all");

        private final String value;

        TransactionSnapshotBoundaryMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be {@code null}
         * @return the matching option, or null if no match is found
         */
        public static TransactionSnapshotBoundaryMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (TransactionSnapshotBoundaryMode option : TransactionSnapshotBoundaryMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be {@code null}
         * @param defaultValue the default value; may be {@code null}
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static TransactionSnapshotBoundaryMode parse(String value, String defaultValue) {
            TransactionSnapshotBoundaryMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }
    
    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public enum SnapshotMode
            implements EnumeratedValue {
        /**
         * Perform a snapshot of data and schema upon initial startup of a connector.
         */
        INITIAL("initial", true),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         */
        SCHEMA_ONLY("schema_only", false);

        private final String value;
        private final boolean includeData;

        SnapshotMode(String value, boolean includeData) {
            this.value = value;
            this.includeData = includeData;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value        the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Whether this snapshotting mode should include the actual data or just the
         * schema of captured tables.
         */
        public boolean includeData() {
            return includeData;
        }
    }

    public enum ConnectorAdapter
            implements EnumeratedValue {
        /**
         * This is based on XStream API.
         */
        XSTREAM("XStream") {
            @Override
            public String getConnectionUrl() {
                return "jdbc:oracle:oci:@${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";
            }
        },
        
        

        /**
         * This is based on LogMiner utility.
         */
        LOG_MINER("LogMiner") {
            @Override
            public String getConnectionUrl() {
                return "jdbc:oracle:thin:@${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";
            }
        };
        
        

        private final String value;

        ConnectorAdapter(String value) {
            this.value = value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static ConnectorAdapter parse(String value) {
            if (value == null) {
                return ConnectorAdapter.LOG_MINER;
            }
            value = value.trim();
            for (ConnectorAdapter adapter : ConnectorAdapter.values()) {
                if (adapter.getValue().equalsIgnoreCase(value)) {
                    return adapter;
                }
            }
            return null;
        }

        public static ConnectorAdapter parse(String value, String defaultValue) {
            ConnectorAdapter mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }

        public abstract String getConnectionUrl();

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum LogMiningStrategy
            implements EnumeratedValue {
        /**
         * This strategy uses LogMiner with data dictionary in online catalog.
         * This option will not capture DDL , but acts fast on REDO LOG switch events
         * This option does not use CONTINUOUS_MINE option
         */
        ONLINE_CATALOG("online_catalog"),

        /**
         * This strategy uses LogMiner with data dictionary in REDO LOG files.
         * This option will capture DDL, but will develop some lag on REDO LOG switch event and will eventually catch up
         * This option does not use CONTINUOUS_MINE option
         * This is default value
         */
        CATALOG_IN_REDO("redo_log_catalog"),

        /**
         * This strategy uses LogMiner with data dictionary located in ORACLE read-only server.
         * This option need the path location of the dictionary file.
         * This option is a combination with the {@code redo_log_catalog} strategy.
         */
        DICTIONARY_FROM_FILE("dictionary_from_file"),

        /**
         * This strategy combines the performance of {@code online_catalog} with the schema capture capabilities of
         * the {@code redo_log_catalog} strategy. If LogMiner fails to reconstruct a DML event, this strategy will
         * default to using Debezium's schema metadata to reconstruct the DML in-flight when LogMiner cannot.
         */
        HYBRID("hybrid");;

        private final String value;

        LogMiningStrategy(String value) {
            this.value = value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static LogMiningStrategy parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (LogMiningStrategy adapter : LogMiningStrategy.values()) {
                if (adapter.getValue().equalsIgnoreCase(value)) {
                    return adapter;
                }
            }
            return null;
        }

        public static LogMiningStrategy parse(String value, String defaultValue) {
            LogMiningStrategy mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public enum LogMiningDmlParser
            implements EnumeratedValue {
        LEGACY("legacy"),
        FAST("fast");

        private final String value;

        LogMiningDmlParser(String value) {
            this.value = value;
        }

        public static LogMiningDmlParser parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (LogMiningDmlParser parser : LogMiningDmlParser.values()) {
                if (parser.getValue().equalsIgnoreCase(value)) {
                    return parser;
                }
            }
            return null;
        }

        public static LogMiningDmlParser parse(String value, String defaultValue) {
            LogMiningDmlParser mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    /**
     * A {@link TableFilter} that excludes all Oracle system tables.
     *
     * @author Gunnar Morling
     */
    private static class SystemTablesPredicate
            implements TableFilter {
        private final Configuration config;

        SystemTablesPredicate(Configuration config) {
            this.config = config;
        }

        @Override
        public boolean isIncluded(TableId t) {
            return !isExcludedSchema(t) && !isFlushTable(t);
        }

        private boolean isExcludedSchema(TableId id) {
            return EXCLUDED_SCHEMAS.contains(id.schema().toLowerCase());
        }

        private boolean isFlushTable(TableId id) {
            final String schema = config.getString(USER);
            return id.table().equalsIgnoreCase(SqlUtils.LOGMNR_FLUSH_TABLE) && id.schema().equalsIgnoreCase(schema);
        }
    }
}
