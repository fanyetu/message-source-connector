package com.capinfo.kafka.connector.source;

import com.capinfo.kafka.connector.util.DateTimeUtils;
import com.capinfo.kafka.connector.util.EnumRecommender;
import com.capinfo.kafka.connector.util.QuoteMethod;
import com.capinfo.kafka.connector.util.TimeZoneValidator;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * @author zhanghaonan
 * @date 2022/2/8
 */
public class MessageSourceConnectorConfig extends AbstractConfig {

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public static final String CONNECTION_PREFIX = "connection.";
    private static Pattern INVALID_CHARS = Pattern.compile("[^a-zA-Z0-9._-]");

    /**
     * 数据库连接URL配置
     */
    public static final String CONNECTION_URL_CONFIG = CONNECTION_PREFIX + "url";
    private static final String CONNECTION_URL_DOC =
            "JDBC connection URL.\n"
                    + "For example: ``jdbc:oracle:thin:@localhost:1521:orclpdb1``, "
                    + "``jdbc:mysql://localhost/db_name``, "
                    + "``jdbc:sqlserver://localhost;instance=SQLEXPRESS;"
                    + "databaseName=db_name``";
    private static final String CONNECTION_URL_DISPLAY = "JDBC URL";

    /**
     * 数据库用户名配置
     */
    public static final String CONNECTION_USER_CONFIG = CONNECTION_PREFIX + "user";
    private static final String CONNECTION_USER_DOC = "JDBC connection user.";
    private static final String CONNECTION_USER_DISPLAY = "JDBC User";

    /**
     * 数据库密码配置
     */
    public static final String CONNECTION_PASSWORD_CONFIG = CONNECTION_PREFIX + "password";
    private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";
    private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

    /**
     * 检索一个有效的JDBC连接的最大尝试次数。必须是一个正整数。
     */
    public static final String CONNECTION_ATTEMPTS_CONFIG = CONNECTION_PREFIX + "attempts";
    public static final String CONNECTION_ATTEMPTS_DOC
            = "Maximum number of attempts to retrieve a valid JDBC connection. "
            + "Must be a positive integer.";
    public static final String CONNECTION_ATTEMPTS_DISPLAY = "JDBC connection attempts";
    public static final int CONNECTION_ATTEMPTS_DEFAULT = 3;

    /**
     * 连接尝试之间的间隔时间，以毫秒为单位。
     */
    public static final String CONNECTION_BACKOFF_CONFIG = CONNECTION_PREFIX + "backoff.ms";
    public static final String CONNECTION_BACKOFF_DOC
            = "Backoff time in milliseconds between connection attempts.";
    public static final String CONNECTION_BACKOFF_DISPLAY
            = "JDBC connection backoff in milliseconds";
    public static final long CONNECTION_BACKOFF_DEFAULT = 10000L;

    /**
     * 目标字段
     */
    public static final String TARGET_COLUMN_CONFIG = "target.column.name";
    public static final String TARGET_COLUMN_CONFIG_DEFAULT = "target";
    public static final String TARGET_COLUMN_CONFIG_DISPLAY = "target column name";
    public static final String TARGET_COLUMN_CONFIG_DOC = "目标字段名称，默认会以topic.prefix+target作为目标topic名称，" +
            "但是如果该条数据没有target，则使用topic.prefix+source作为topic名称";

    /**
     * 源字段
     */
    public static final String SOURCE_COLUMN_CONFIG = "source.column.name";
    public static final String SOURCE_COLUMN_CONFIG_DEFAULT = "source";
    public static final String SOURCE_COLUMN_CONFIG_DISPLAY = "source column name";
    public static final String SOURCE_COLUMN_CONFIG_DOC = "源字段名称，默认会以topic.prefix+target作为目标topic名称，" +
            "但是如果该条数据没有target，则使用topic.prefix+source作为topic名称";

    public static final String NUMERIC_PRECISION_MAPPING_CONFIG = "numeric.precision.mapping";
    private static final String NUMERIC_PRECISION_MAPPING_DOC =
            "Whether or not to attempt mapping NUMERIC values by precision to integral types. This "
                    + "option is now deprecated. A future version may remove it completely. Please use "
                    + "``numeric.mapping`` instead.";

    public static final boolean NUMERIC_PRECISION_MAPPING_DEFAULT = false;
    public static final String NUMERIC_MAPPING_CONFIG = "numeric.mapping";
    private static final String NUMERIC_PRECISION_MAPPING_DISPLAY = "Map Numeric Values By "
            + "Precision (deprecated)";

    private static final String NUMERIC_MAPPING_DOC =
            "Map NUMERIC values by precision and optionally scale to integral or decimal types.\n"
                    + "  * Use ``none`` if all NUMERIC columns are to be represented by Connect's DECIMAL "
                    + "logical type.\n"
                    + "  * Use ``best_fit`` if NUMERIC columns should be cast to Connect's INT8, INT16, "
                    + "INT32, INT64, or FLOAT64 based upon the column's precision and scale. This option may "
                    + "still represent the NUMERIC value as Connect DECIMAL if it cannot be cast to a native "
                    + "type without losing precision. For example, a NUMERIC(20) type with precision 20 would "
                    + "not be able to fit in a native INT64 without overflowing and thus would be retained as "
                    + "DECIMAL.\n"
                    + "  * Use ``best_fit_eager_double`` if in addition to the properties of ``best_fit`` "
                    + "described above, it is desirable to always cast NUMERIC columns with scale to Connect "
                    + "FLOAT64 type, despite potential of loss in accuracy.\n"
                    + "  * Use ``precision_only`` to map NUMERIC columns based only on the column's precision "
                    + "assuming that column's scale is 0.\n"
                    + "  * The ``none`` option is the default, but may lead to serialization issues with Avro "
                    + "since Connect's DECIMAL type is mapped to its binary representation, and ``best_fit`` "
                    + "will often be preferred since it maps to the most appropriate primitive type.";

    public static final String NUMERIC_MAPPING_DEFAULT = null;
    private static final String NUMERIC_MAPPING_DISPLAY = "Map Numeric Values, Integral "
            + "or Decimal, By Precision and Scale";

    private static final EnumRecommender NUMERIC_MAPPING_RECOMMENDER =
            EnumRecommender.in(NumericMapping.values());

    /**
     * 定义Timestamp列的颗粒度。选项包括。
     * connect_logical (default): 使用Kafka Connect的内置表示法表示时间戳值。
     * nanos_long: 表示时间戳值为自纪元以来的纳米数。
     * nanos_string: 用字符串表示时间戳值，即自纪元以来的纳米数。
     * nanos_iso_datetime_string: 使用iso格式'yyyy-MM-dd'T'HH:mm:ss.n'
     */
    public static final String TIMESTAMP_GRANULARITY_CONFIG = "timestamp.granularity";
    public static final String TIMESTAMP_GRANULARITY_DOC =
            "Define the granularity of the Timestamp column. Options include: \n"
                    + "  * connect_logical (default): represents timestamp values using Kafka Connect's "
                    + "built-in representations \n"
                    + "  * nanos_long: represents timestamp values as nanos since epoch\n"
                    + "  * nanos_string: represents timestamp values as nanos since epoch in string\n"
                    + "  * nanos_iso_datetime_string: uses the iso format 'yyyy-MM-dd'T'HH:mm:ss.n'\n";
    public static final String TIMESTAMP_GRANULARITY_DISPLAY = "Timestamp granularity for "
            + "timestamp columns";
    private static final EnumRecommender TIMESTAMP_GRANULARITY_RECOMMENDER =
            EnumRecommender.in(TimestampGranularity.values());

    public static final String DB_TIMEZONE_CONFIG = "db.timezone";
    public static final String DB_TIMEZONE_DEFAULT = "UTC";
    private static final String DB_TIMEZONE_CONFIG_DOC =
            "Name of the JDBC timezone used in the connector when "
                    + "querying with time-based criteria. Defaults to UTC.";
    private static final String DB_TIMEZONE_CONFIG_DISPLAY = "DB time zone";

    public static final String CATALOG_PATTERN_CONFIG = "catalog.pattern";
    private static final String CATALOG_PATTERN_DOC =
            "Catalog pattern to fetch table metadata from the database.\n"
                    + "  * ``\"\"`` retrieves those without a catalog \n"
                    + "  * null (default) indicates that the schema name is not used to narrow the search and "
                    + "that all table metadata is fetched, regardless of the catalog.";
    private static final String CATALOG_PATTERN_DISPLAY = "Schema pattern";
    public static final String CATALOG_PATTERN_DEFAULT = null;

    public static final String SCHEMA_PATTERN_CONFIG = "schema.pattern";
    private static final String SCHEMA_PATTERN_DOC =
            "Schema pattern to fetch table metadata from the database.\n"
                    + "  * ``\"\"`` retrieves those without a schema.\n"
                    + "  * null (default) indicates that the schema name is not used to narrow the search and "
                    + "that all table metadata is fetched, regardless of the schema.";
    private static final String SCHEMA_PATTERN_DISPLAY = "Schema pattern";
    public static final String SCHEMA_PATTERN_DEFAULT = null;

    /**
     * 未使用该配置
     */
    public static final String TABLE_TYPE_DEFAULT = "TABLE";
    public static final String TABLE_TYPE_CONFIG = "table.types";
    private static final String TABLE_TYPE_DOC =
            "By default, the JDBC connector will only detect tables with type TABLE from the source "
                    + "Database. This config allows a command separated list of table types to extract. Options"
                    + " include:\n"
                    + "  * TABLE\n"
                    + "  * VIEW\n"
                    + "  * SYSTEM TABLE\n"
                    + "  * GLOBAL TEMPORARY\n"
                    + "  * LOCAL TEMPORARY\n"
                    + "  * ALIAS\n"
                    + "  * SYNONYM\n"
                    + "  In most cases it only makes sense to have either TABLE or VIEW.";
    private static final String TABLE_TYPE_DISPLAY = "Table Types";

    public static final String QUOTE_SQL_IDENTIFIERS_CONFIG = "quote.sql.identifiers";
    public static final String QUOTE_SQL_IDENTIFIERS_DEFAULT = "always";
    public static final String QUOTE_SQL_IDENTIFIERS_DOC =
            "When to quote table names, column names, and other identifiers in SQL statements. "
                    + "For backward compatibility, the default is ``always``.";
    public static final String QUOTE_SQL_IDENTIFIERS_DISPLAY = "Quote Identifiers";

    public static final String BATCH_MAX_ROWS_CONFIG = "batch.max.rows";
    private static final String BATCH_MAX_ROWS_DOC =
            "Maximum number of rows to include in a single batch when polling for new data. This "
                    + "setting can be used to limit the amount of data buffered internally in the connector.";
    public static final int BATCH_MAX_ROWS_DEFAULT = 100;
    private static final String BATCH_MAX_ROWS_DISPLAY = "Max Rows Per Batch";

    /**
     * 轮询新表或移除表的频率，这可能导致更新任务配置，以开始轮询新增表的数据或停止轮询移除表的数据。
     * 在{@link com.capinfo.kafka.connector.MessageSourceConnector}中我们没有使用这个配置，
     * 因为我们只需要读取一张表
     */
    public static final String TABLE_POLL_INTERVAL_MS_CONFIG = "table.poll.interval.ms";
    private static final String TABLE_POLL_INTERVAL_MS_DOC =
            "Frequency in ms to poll for new or removed tables, which may result in updated task "
                    + "configurations to start polling for data in added tables or stop polling for data in "
                    + "removed tables.";
    public static final long TABLE_POLL_INTERVAL_MS_DEFAULT = 60 * 1000;
    private static final String TABLE_POLL_INTERVAL_MS_DISPLAY
            = "Metadata Change Monitoring Interval (ms)";

    public static final String QUERY_CONFIG = "query";
    private static final String QUERY_DOC =
            "If specified, the query to perform to select new or updated rows. Use this setting if you "
                    + "want to join tables, select subsets of columns in a table, or filter data. If used, this"
                    + " connector will only copy data using this query -- whole-table copying will be disabled."
                    + " Different query modes may still be used for incremental updates, but in order to "
                    + "properly construct the incremental query, it must be possible to append a WHERE clause "
                    + "to this query (i.e. no WHERE clauses may be used). If you use a WHERE clause, it must "
                    + "handle incremental queries itself.";
    public static final String QUERY_DEFAULT = "";
    private static final String QUERY_DISPLAY = "Query";

    /**
     * 每次轮询时更新一个表的模式
     * bulk：每次轮询时对整个表进行批量加载。 ====>对于MessageSourceConnector，没有这个选项
     * incrementing：在每个表上使用一个严格的递增列，只检测新的行。注意，这不会检测到对现有记录的修改或删除。
     * timestamp：使用一个时间戳（或类似时间戳的）列来检测新的和修改的记录。这假定该列在每次写入时都会被更新，而且数值是单调递增的，但不一定是唯一的。
     * timestamp+incrementing：使用两个列，一个是检测新的和修改过的记录的时间戳列，另一个是严格递增的列，为更新提供一个全局唯一的ID，这样每条记录就可以被分配一个唯一的流偏移。
     */
    public static final String MODE_CONFIG = "mode";
    private static final String MODE_DOC =
            "The mode for updating a table each time it is polled. Options include:\n"
                    + "  * bulk: perform a bulk load of the entire table each time it is polled\n"
                    + "  * incrementing: use a strictly incrementing column on each table to "
                    + "detect only new rows. Note that this will not detect modifications or "
                    + "deletions of existing rows.\n"
                    + "  * timestamp: use a timestamp (or timestamp-like) column to detect new and modified "
                    + "rows. This assumes the column is updated with each write, and that values are "
                    + "monotonically incrementing, but not necessarily unique.\n"
                    + "  * timestamp+incrementing: use two columns, a timestamp column that detects new and "
                    + "modified rows and a strictly incrementing column which provides a globally unique ID for "
                    + "updates so each row can be assigned a unique stream offset.";
    private static final String MODE_DISPLAY = "Table Loading Mode";

    public static final String MODE_UNSPECIFIED = "";
    public static final String MODE_BULK = "bulk";
    public static final String MODE_TIMESTAMP = "timestamp";
    public static final String MODE_INCREMENTING = "incrementing";
    public static final String MODE_TIMESTAMP_INCREMENTING = "timestamp+incrementing";

    public static final String INCREMENTING_COLUMN_NAME_CONFIG = "incrementing.column.name";
    private static final String INCREMENTING_COLUMN_NAME_DOC =
            "The name of the strictly incrementing column to use to detect new rows. Any empty value "
                    + "indicates the column should be autodetected by looking for an auto-incrementing column. "
                    + "This column may not be nullable.";
    public static final String INCREMENTING_COLUMN_NAME_DEFAULT = "";
    private static final String INCREMENTING_COLUMN_NAME_DISPLAY = "Incrementing Column Name";

    public static final String TIMESTAMP_COLUMN_NAME_CONFIG = "timestamp.column.name";
    private static final String TIMESTAMP_COLUMN_NAME_DOC =
            "Comma separated list of one or more timestamp columns to detect new or modified rows using "
                    + "the COALESCE SQL function. Rows whose first non-null timestamp value is greater than the "
                    + "largest previous timestamp value seen will be discovered with each poll. At least one "
                    + "column should not be nullable.";
    public static final String TIMESTAMP_COLUMN_NAME_DEFAULT = "";
    private static final String TIMESTAMP_COLUMN_NAME_DISPLAY = "Timestamp Column Name";

    /**
     * 在具有特定时间戳的行出现后，我们要等待多长时间才能将其纳入结果中。你可以选择添加一些延迟，
     * 以允许具有较早时间戳的事务完成。第一次执行将获取所有可用记录（即从时间戳0开始），
     * 直到当前时间减去延迟。接下来的每一次执行将获取从最后一次获取到当前时间减去延迟的数据。
     */
    public static final String TIMESTAMP_DELAY_INTERVAL_MS_CONFIG = "timestamp.delay.interval.ms";
    private static final String TIMESTAMP_DELAY_INTERVAL_MS_DOC =
            "How long to wait after a row with certain timestamp appears before we include it in the "
                    + "result. You may choose to add some delay to allow transactions with earlier timestamp to"
                    + " complete. The first execution will fetch all available records (i.e. starting at "
                    + "timestamp 0) until current time minus the delay. Every following execution will get data"
                    + " from the last time we fetched until current time minus the delay.";
    public static final long TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT = 0;
    private static final String TIMESTAMP_DELAY_INTERVAL_MS_DISPLAY = "Delay Interval (ms)";

    public static final String QUERY_SUFFIX_CONFIG = "query.suffix";
    public static final String QUERY_SUFFIX_DEFAULT = "";
    public static final String QUERY_SUFFIX_DOC =
            "Suffix to append at the end of the generated query.";
    public static final String QUERY_SUFFIX_DISPLAY = "Query suffix";

    /**
     * 主题的前缀，这里会结合target字段或source字段生成kafka主题
     */
    public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
    private static final String TOPIC_PREFIX_DOC =
            "Prefix to prepend to table names to generate the name of the Kafka topic to publish data "
                    + "to, or in the case of a custom query, the full name of the topic to publish to.";
    private static final String TOPIC_PREFIX_DISPLAY = "Topic Prefix";

    public static final String TIMESTAMP_INITIAL_CONFIG = "timestamp.initial";
    public static final Long TIMESTAMP_INITIAL_DEFAULT = null;
    public static final Long TIMESTAMP_INITIAL_CURRENT = Long.valueOf(-1);
    public static final String TIMESTAMP_INITIAL_DOC =
            "The epoch timestamp used for initial queries that use timestamp criteria. "
                    + "Use -1 to use the current time. If not specified, all data will be retrieved.";
    public static final String TIMESTAMP_INITIAL_DISPLAY = "Unix time value of initial timestamp";

    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in "
            + "each table.";
    public static final int POLL_INTERVAL_MS_DEFAULT = 5000;
    private static final String POLL_INTERVAL_MS_DISPLAY = "Poll Interval (ms)";

    public MessageSourceConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    public MessageSourceConnectorConfig(Map<String, String> configProperties) {
        super(CONFIG_DEF, configProperties);
    }

    public static ConfigDef baseConfigDef() {
        ConfigDef config = new ConfigDef();
        addDatabaseOptions(config);
        addModeOptions(config);
        addConnectorOptions(config);
        return config;
    }

    public static final String DATABASE_GROUP = "Database";
    public static final String MODE_GROUP = "Mode";
    public static final String CONNECTOR_GROUP = "Connector";

    private static final void addDatabaseOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                CONNECTION_URL_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                CONNECTION_URL_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                CONNECTION_URL_DISPLAY
        ).define(
                CONNECTION_USER_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                CONNECTION_USER_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                CONNECTION_USER_DISPLAY
        ).define(
                CONNECTION_PASSWORD_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                ConfigDef.Importance.HIGH,
                CONNECTION_PASSWORD_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                CONNECTION_PASSWORD_DISPLAY
        ).define(
                CONNECTION_ATTEMPTS_CONFIG,
                ConfigDef.Type.INT,
                CONNECTION_ATTEMPTS_DEFAULT,
                ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.LOW,
                CONNECTION_ATTEMPTS_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                CONNECTION_ATTEMPTS_DISPLAY
        ).define(
                CONNECTION_BACKOFF_CONFIG,
                ConfigDef.Type.LONG,
                CONNECTION_BACKOFF_DEFAULT,
                ConfigDef.Importance.LOW,
                CONNECTION_BACKOFF_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                CONNECTION_BACKOFF_DISPLAY
        ).define(
                CATALOG_PATTERN_CONFIG,
                ConfigDef.Type.STRING,
                CATALOG_PATTERN_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                CATALOG_PATTERN_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                CATALOG_PATTERN_DISPLAY
        ).define(
                SCHEMA_PATTERN_CONFIG,
                ConfigDef.Type.STRING,
                SCHEMA_PATTERN_DEFAULT,
                ConfigDef.Importance.HIGH,
                SCHEMA_PATTERN_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                SCHEMA_PATTERN_DISPLAY
        ).define(
                NUMERIC_PRECISION_MAPPING_CONFIG,
                ConfigDef.Type.BOOLEAN,
                NUMERIC_PRECISION_MAPPING_DEFAULT,
                ConfigDef.Importance.LOW,
                NUMERIC_PRECISION_MAPPING_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                NUMERIC_PRECISION_MAPPING_DISPLAY
        ).define(
                NUMERIC_MAPPING_CONFIG,
                ConfigDef.Type.STRING,
                NUMERIC_MAPPING_DEFAULT,
                NUMERIC_MAPPING_RECOMMENDER,
                ConfigDef.Importance.LOW,
                NUMERIC_MAPPING_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                NUMERIC_MAPPING_DISPLAY,
                NUMERIC_MAPPING_RECOMMENDER
        );
    }

    private static final ConfigDef.Recommender MODE_DEPENDENTS_RECOMMENDER = new ModeDependentsRecommender();

    private static final EnumRecommender QUOTE_METHOD_RECOMMENDER =
            EnumRecommender.in(QuoteMethod.values());

    private static final void addModeOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                MODE_CONFIG,
                ConfigDef.Type.STRING,
                MODE_UNSPECIFIED,
                ConfigDef.ValidString.in(
                        MODE_UNSPECIFIED,
                        MODE_BULK,
                        MODE_TIMESTAMP,
                        MODE_INCREMENTING,
                        MODE_TIMESTAMP_INCREMENTING
                ),
                ConfigDef.Importance.HIGH,
                MODE_DOC,
                MODE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                MODE_DISPLAY,
                Arrays.asList(
                        INCREMENTING_COLUMN_NAME_CONFIG,
                        TIMESTAMP_COLUMN_NAME_CONFIG
                )
        ).define(
                INCREMENTING_COLUMN_NAME_CONFIG,
                ConfigDef.Type.STRING,
                INCREMENTING_COLUMN_NAME_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                INCREMENTING_COLUMN_NAME_DOC,
                MODE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                INCREMENTING_COLUMN_NAME_DISPLAY,
                MODE_DEPENDENTS_RECOMMENDER
        ).define(
                TIMESTAMP_COLUMN_NAME_CONFIG,
                ConfigDef.Type.LIST,
                TIMESTAMP_COLUMN_NAME_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                TIMESTAMP_COLUMN_NAME_DOC,
                MODE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                TIMESTAMP_COLUMN_NAME_DISPLAY,
                MODE_DEPENDENTS_RECOMMENDER
        ).define(
                TIMESTAMP_INITIAL_CONFIG,
                ConfigDef.Type.LONG,
                TIMESTAMP_INITIAL_DEFAULT,
                ConfigDef.Importance.LOW,
                TIMESTAMP_INITIAL_DOC,
                MODE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                TIMESTAMP_INITIAL_DISPLAY,
                MODE_DEPENDENTS_RECOMMENDER
        ).define(
                QUERY_CONFIG,
                ConfigDef.Type.STRING,
                QUERY_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                QUERY_DOC,
                MODE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                QUERY_DISPLAY
        ).define(
                QUOTE_SQL_IDENTIFIERS_CONFIG,
                ConfigDef.Type.STRING,
                QUOTE_SQL_IDENTIFIERS_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                QUOTE_SQL_IDENTIFIERS_DOC,
                MODE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                QUOTE_SQL_IDENTIFIERS_DISPLAY,
                QUOTE_METHOD_RECOMMENDER
        ).define(
                QUERY_SUFFIX_CONFIG,
                ConfigDef.Type.STRING,
                QUERY_SUFFIX_DEFAULT,
                ConfigDef.Importance.LOW,
                QUERY_SUFFIX_DOC,
                MODE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                QUERY_SUFFIX_DISPLAY);
    }

    private static final void addConnectorOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(SOURCE_COLUMN_CONFIG,
                ConfigDef.Type.STRING,
                SOURCE_COLUMN_CONFIG_DEFAULT,
                ConfigDef.Importance.HIGH,
                SOURCE_COLUMN_CONFIG_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                SOURCE_COLUMN_CONFIG_DISPLAY
        ).define(
                TARGET_COLUMN_CONFIG,
                ConfigDef.Type.STRING,
                TARGET_COLUMN_CONFIG_DEFAULT,
                ConfigDef.Importance.HIGH,
                TARGET_COLUMN_CONFIG_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                TARGET_COLUMN_CONFIG_DISPLAY
        ).define(
                TABLE_TYPE_CONFIG,
                ConfigDef.Type.LIST,
                TABLE_TYPE_DEFAULT,
                ConfigDef.Importance.LOW,
                TABLE_TYPE_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                TABLE_TYPE_DISPLAY
        ).define(
                POLL_INTERVAL_MS_CONFIG,
                ConfigDef.Type.INT,
                POLL_INTERVAL_MS_DEFAULT,
                ConfigDef.Importance.HIGH,
                POLL_INTERVAL_MS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                POLL_INTERVAL_MS_DISPLAY
        ).define(
                BATCH_MAX_ROWS_CONFIG,
                ConfigDef.Type.INT,
                BATCH_MAX_ROWS_DEFAULT,
                ConfigDef.Importance.LOW,
                BATCH_MAX_ROWS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                BATCH_MAX_ROWS_DISPLAY
        ).define(
                TABLE_POLL_INTERVAL_MS_CONFIG,
                ConfigDef.Type.LONG,
                TABLE_POLL_INTERVAL_MS_DEFAULT,
                ConfigDef.Importance.LOW,
                TABLE_POLL_INTERVAL_MS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                TABLE_POLL_INTERVAL_MS_DISPLAY
        ).define(
                TOPIC_PREFIX_CONFIG,
                ConfigDef.Type.STRING,
                "",
//                new ConfigDef.Validator() {
//                    @Override
//                    public void ensureValid(final String name, final Object value) {
//                        if (value == null) {
//                            throw new ConfigException(name, value, "Topic prefix must not be null.");
//                        }
//
//                        String trimmed = ((String) value).trim();
//
//                        if (trimmed.length() > 249) {
//                            throw new ConfigException(name, value,
//                                    "Topic prefix length must not exceed max topic name length, 249 chars");
//                        }
//
//                        if (INVALID_CHARS.matcher(trimmed).find()) {
//                            throw new ConfigException(name, value,
//                                    "Topic prefix must not contain any character other than "
//                                            + "ASCII alphanumerics, '.', '_' and '-'.");
//                        }
//                    }
//                },
                ConfigDef.Importance.HIGH,
                TOPIC_PREFIX_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                TOPIC_PREFIX_DISPLAY
        ).define(
                TIMESTAMP_DELAY_INTERVAL_MS_CONFIG,
                ConfigDef.Type.LONG,
                TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT,
                ConfigDef.Importance.HIGH,
                TIMESTAMP_DELAY_INTERVAL_MS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                TIMESTAMP_DELAY_INTERVAL_MS_DISPLAY
        ).define(
                DB_TIMEZONE_CONFIG,
                ConfigDef.Type.STRING,
                DB_TIMEZONE_DEFAULT,
                TimeZoneValidator.INSTANCE,
                ConfigDef.Importance.MEDIUM,
                DB_TIMEZONE_CONFIG_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                DB_TIMEZONE_CONFIG_DISPLAY
        ).define(
                TIMESTAMP_GRANULARITY_CONFIG,
                ConfigDef.Type.STRING,
                TimestampGranularity.DEFAULT,
                TIMESTAMP_GRANULARITY_RECOMMENDER,
                ConfigDef.Importance.LOW,
                TIMESTAMP_GRANULARITY_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                TIMESTAMP_GRANULARITY_DISPLAY,
                TIMESTAMP_GRANULARITY_RECOMMENDER);
    }


    private static class ModeDependentsRecommender implements ConfigDef.Recommender {

        @Override
        public List<Object> validValues(String name, Map<String, Object> config) {
            return new LinkedList<>();
        }

        @Override
        public boolean visible(String name, Map<String, Object> config) {
            String mode = (String) config.get(MODE_CONFIG);
            switch (mode) {
                case MODE_BULK:
                    return false;
                case MODE_TIMESTAMP:
                    return name.equals(TIMESTAMP_COLUMN_NAME_CONFIG);
                case MODE_INCREMENTING:
                    return name.equals(INCREMENTING_COLUMN_NAME_CONFIG);
                case MODE_TIMESTAMP_INCREMENTING:
                    return name.equals(TIMESTAMP_COLUMN_NAME_CONFIG)
                            || name.equals(INCREMENTING_COLUMN_NAME_CONFIG);
                case MODE_UNSPECIFIED:
                    throw new ConfigException("Query mode must be specified");
                default:
                    throw new ConfigException("Invalid mode: " + mode);
            }
        }
    }

    public String getSourceColumn() {
        String source = getString(SOURCE_COLUMN_CONFIG);
        return StringUtils.isBlank(source) ? SOURCE_COLUMN_CONFIG_DEFAULT : source;
    }

    public String getTargetColumn() {
        String target = getString(TARGET_COLUMN_CONFIG);
        return StringUtils.isBlank(target) ? TARGET_COLUMN_CONFIG_DEFAULT : target;
    }

    public String topicPrefix() {
        return getString(TOPIC_PREFIX_CONFIG).trim();
    }

    public enum NumericMapping {
        NONE,
        PRECISION_ONLY,
        BEST_FIT,
        BEST_FIT_EAGER_DOUBLE;

        private static final Map<String, NumericMapping> reverse = new HashMap<>(values().length);

        static {
            for (NumericMapping val : values()) {
                reverse.put(val.name().toLowerCase(Locale.ROOT), val);
            }
        }

        public static NumericMapping get(String prop) {
            // not adding a check for null value because the recommender/validator should catch those.
            return reverse.get(prop.toLowerCase(Locale.ROOT));
        }

        public static NumericMapping get(MessageSourceConnectorConfig config) {
            String newMappingConfig = config.getString(MessageSourceConnectorConfig.NUMERIC_MAPPING_CONFIG);
            // We use 'null' as default to be able to check the old config if the new one is unset.
            if (newMappingConfig != null) {
                return get(config.getString(MessageSourceConnectorConfig.NUMERIC_MAPPING_CONFIG));
            }
            if (config.getBoolean(MessageSourceTaskConfig.NUMERIC_PRECISION_MAPPING_CONFIG)) {
                return NumericMapping.PRECISION_ONLY;
            }
            return NumericMapping.NONE;
        }
    }

    public enum TimestampGranularity {
        CONNECT_LOGICAL(optional -> optional
                ? org.apache.kafka.connect.data.Timestamp.builder().optional().build()
                : org.apache.kafka.connect.data.Timestamp.builder().build(),
                timestamp -> timestamp,
                timestamp -> (Timestamp) timestamp),

        NANOS_LONG(optional -> optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA,
                DateTimeUtils::toEpochNanos,
                epochNanos -> DateTimeUtils.toTimestamp((Long) epochNanos)),

        NANOS_STRING(optional -> optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA,
                DateTimeUtils::toEpochNanosString,
                epochNanosString -> {
                    try {
                        return DateTimeUtils.toTimestamp((String) epochNanosString);
                    } catch (NumberFormatException e) {
                        throw new ConnectException(
                                "Invalid value for timestamp column with nanos-string granularity: "
                                        + epochNanosString
                                        + e.getMessage());
                    }
                }),

        NANOS_ISO_DATETIME_STRING(optional -> optional
                ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA,
                DateTimeUtils::toIsoDateTimeString,
                isoDateTimeString -> DateTimeUtils.toTimestampFromIsoDateTime((String) isoDateTimeString));

        public final Function<Boolean, Schema> schemaFunction;
        public final Function<Timestamp, Object> fromTimestamp;
        public final Function<Object, Timestamp> toTimestamp;

        public static final String DEFAULT = CONNECT_LOGICAL.name().toLowerCase(Locale.ROOT);

        private static final Map<String, TimestampGranularity> reverse = new HashMap<>(values().length);

        static {
            for (TimestampGranularity val : values()) {
                reverse.put(val.name().toLowerCase(Locale.ROOT), val);
            }
        }

        public static TimestampGranularity get(MessageSourceConnectorConfig config) {
            String tsGranularity = config.getString(TIMESTAMP_GRANULARITY_CONFIG);
            return reverse.get(tsGranularity.toLowerCase(Locale.ROOT));
        }

        TimestampGranularity(Function<Boolean, Schema> schemaFunction,
                             Function<Timestamp, Object> fromTimestamp,
                             Function<Object, Timestamp> toTimestamp) {
            this.schemaFunction = schemaFunction;
            this.fromTimestamp = fromTimestamp;
            this.toTimestamp = toTimestamp;
        }
    }

    public NumericMapping numericMapping() {
        return NumericMapping.get(this);
    }

    public TimeZone timeZone() {
        String dbTimeZone = getString(MessageSourceTaskConfig.DB_TIMEZONE_CONFIG);
        return TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
    }
}
