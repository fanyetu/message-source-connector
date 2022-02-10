package com.capinfo.kafka.connector.source;

import com.capinfo.kafka.connector.dialect.DatabaseDialect;
import com.capinfo.kafka.connector.dialect.DatabaseDialects;
import com.capinfo.kafka.connector.util.*;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author zhanghaonan
 * @date 2022/2/7
 */
public class MessageSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MessageSourceTask.class);
    // When no results, periodically return control flow to caller to give it a chance to pause us.
    private static final int CONSECUTIVE_EMPTY_RESULTS_BEFORE_RETURN = 3;

    private Time time;
    private MessageSourceTaskConfig config;
    private DatabaseDialect dialect;
    private CachedConnectionProvider cachedConnectionProvider;
    private PriorityQueue<TableQuerier> tableQueue = new PriorityQueue<TableQuerier>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong taskThreadId = new AtomicLong(0);

    public MessageSourceTask() {
        this.time = new SystemTime();
    }

    public MessageSourceTask(Time time) {
        this.time = time;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    /**
     * Task生命周期方法，启动Task
     *
     * @param properties
     */
    @Override
    public void start(Map<String, String> properties) {
        log.info("启动MessageSourceTask");
        try {
            config = new MessageSourceTaskConfig(properties);
        } catch (ConfigException e) {
            throw new ConnectException("配置加载失败，无法启动MessageSourceTask", e);
        }

        final String url = config.getString(MessageSourceConnectorConfig.CONNECTION_URL_CONFIG);
        final int maxConnAttempts = config.getInt(MessageSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
        final long retryBackoff = config.getLong(MessageSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);

        // 查找最合适的数据库实现
        dialect = DatabaseDialects.findBestFor(url, config);
        log.info("使用JDBC数据库实现：{}", dialect.name());

        cachedConnectionProvider = connectionProvider(maxConnAttempts, retryBackoff);

        // 使用query进行查询
        String query = config.getString(MessageSourceTaskConfig.QUERY_CONFIG);
        if (query.isEmpty()) {
            throw new ConnectException("query属性未配置，无法启动该connector");
        }
        TableQuerier.QueryMode queryMode = TableQuerier.QueryMode.QUERY;
        List<String> tablesOrQuery = Collections.singletonList(query);

        // 获取模式
        String mode = config.getString(MessageSourceTaskConfig.MODE_CONFIG);

        Map<Map<String, String>, Map<String, Object>> offsets = null;
        if (mode.equals(MessageSourceTaskConfig.MODE_INCREMENTING)
                || mode.equals(MessageSourceTaskConfig.MODE_TIMESTAMP)
                || mode.equals(MessageSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {

            List<Map<String, String>> partitions = new ArrayList<>(8);
            // Find possible partition maps for different offset protocols. We need to search by all offset protocol partition keys to support compatibility
            log.info("以Query模式启动");
            partitions.add(Collections.singletonMap(MessageSourceConnectorConstants.QUERY_NAME_KEY,
                    MessageSourceConnectorConstants.QUERY_NAME_VALUE));
            offsets = context.offsetStorageReader().offsets(partitions);
            log.info("The partition offsets are {}", offsets);
        }

        String incrementingColumn
                = config.getString(MessageSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG);
        List<String> timestampColumns
                = config.getList(MessageSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
        Long timestampDelayInterval
                = config.getLong(MessageSourceTaskConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG);
        TimeZone timeZone = config.timeZone();
        String suffix = config.getString(MessageSourceTaskConfig.QUERY_SUFFIX_CONFIG).trim();

        for (String tableOrQuery : tablesOrQuery) {
            final List<Map<String, String>> tablePartitionsToCheck;
            final Map<String, String> partition;
            partition = Collections.singletonMap(
                    MessageSourceConnectorConstants.QUERY_NAME_KEY,
                    MessageSourceConnectorConstants.QUERY_NAME_VALUE
            );
            tablePartitionsToCheck = Collections.singletonList(partition);

            // 分区图因偏移协议而异。由于我们不知道每个表的偏移量是由哪个协议决定的，
            // 我们需要使用不同的可能分区(首先是最新的协议版本）来找到每个表的实际偏移量。
            Map<String, Object> offset = null;
            if (offsets != null) {
                for (Map<String, String> toCheckPartition : tablePartitionsToCheck) {
                    offset = offsets.get(toCheckPartition);
                    if (offset != null) {
                        log.info("Found offset {} for partition {}", offsets, toCheckPartition);
                        break;
                    }
                }
            }
            offset = computeInitialOffset(tableOrQuery, offset, timeZone);

            String topicPrefix = config.topicPrefix();
            String sourceColumnName = config.getSourceColumn();
            String targetColumnName = config.getTargetColumn();
            MessageSourceConnectorConfig.TimestampGranularity timestampGranularity
                    = MessageSourceConnectorConfig.TimestampGranularity.get(config);

            /*if (mode.equals(MessageSourceTaskConfig.MODE_BULK)) {
                tableQueue.add(
                        new BulkTableQuerier(
                                dialect,
                                queryMode,
                                tableOrQuery,
                                topicPrefix,
                                suffix
                        )
                );
            } else */
            if (mode.equals(MessageSourceTaskConfig.MODE_INCREMENTING)) {
                tableQueue.add(
                        new TimestampIncrementingTableQuerier(
                                dialect,
                                queryMode,
                                tableOrQuery,
                                topicPrefix,
                                null,
                                incrementingColumn,
                                offset,
                                timestampDelayInterval,
                                timeZone,
                                suffix,
                                timestampGranularity,
                                sourceColumnName,
                                targetColumnName
                        )
                );
            } else if (mode.equals(MessageSourceTaskConfig.MODE_TIMESTAMP)) {
                tableQueue.add(
                        new TimestampTableQuerier(
                                dialect,
                                queryMode,
                                tableOrQuery,
                                topicPrefix,
                                timestampColumns,
                                offset,
                                timestampDelayInterval,
                                timeZone,
                                suffix,
                                timestampGranularity,
                                sourceColumnName,
                                targetColumnName
                        )
                );
            } else if (mode.endsWith(MessageSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
                tableQueue.add(
                        new TimestampIncrementingTableQuerier(
                                dialect,
                                queryMode,
                                tableOrQuery,
                                topicPrefix,
                                timestampColumns,
                                incrementingColumn,
                                offset,
                                timestampDelayInterval,
                                timeZone,
                                suffix,
                                timestampGranularity,
                                sourceColumnName,
                                targetColumnName
                        )
                );
            }
        }

        running.set(true);
        taskThreadId.set(Thread.currentThread().getId());
        log.info("Message Source Task启动");
    }


    protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        return new CachedConnectionProvider(dialect, maxConnAttempts, retryBackoff) {
            @Override
            protected void onConnect(final Connection connection) throws SQLException {
                super.onConnect(connection);
                connection.setAutoCommit(false);
            }
        };
    }

    protected Map<String, Object> computeInitialOffset(
            String tableOrQuery,
            Map<String, Object> partitionOffset,
            TimeZone timezone) {
        if (!(partitionOffset == null)) {
            return partitionOffset;
        } else {
            Map<String, Object> initialPartitionOffset = null;
            // no offsets found
            Long timestampInitial = config.getLong(MessageSourceConnectorConfig.TIMESTAMP_INITIAL_CONFIG);
            if (timestampInitial != null) {
                // start at the specified timestamp
                if (timestampInitial == MessageSourceConnectorConfig.TIMESTAMP_INITIAL_CURRENT) {
                    // use the current time
                    try {
                        final Connection con = cachedConnectionProvider.getConnection();
                        Calendar cal = Calendar.getInstance(timezone);
                        timestampInitial = dialect.currentTimeOnDB(con, cal).getTime();
                    } catch (SQLException e) {
                        throw new ConnectException("Error while getting initial timestamp from database", e);
                    }
                }
                initialPartitionOffset = new HashMap<String, Object>();
                initialPartitionOffset.put(TimestampIncrementingOffset.TIMESTAMP_FIELD, timestampInitial);
                log.info("No offsets found for '{}', so using configured timestamp {}", tableOrQuery,
                        timestampInitial);
            }
            return initialPartitionOffset;
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        log.info("推送新数据");

        Map<TableQuerier, Integer> consecutiveEmptyResults = tableQueue.stream().collect(
                Collectors.toMap(Function.identity(), (q) -> 0));
        while (running.get()) {
            final TableQuerier querier = tableQueue.peek();

            if (!querier.querying()) {
                // If not in the middle of an update, wait for next update time
                final long nextUpdate = querier.getLastUpdate()
                        + config.getInt(MessageSourceTaskConfig.POLL_INTERVAL_MS_CONFIG);
                final long now = time.milliseconds();
                final long sleepMs = Math.min(nextUpdate - now, 100);

                if (sleepMs > 0) {
                    log.debug("Waiting {} ms to poll {} next", nextUpdate - now, querier.toString());
                    time.sleep(sleepMs);
                    continue; // Re-check stop flag before continuing
                }
            }

            final List<SourceRecord> results = new ArrayList<>();
            try {
                log.debug("Checking for next block of results from {}", querier.toString());
                querier.maybeStartQuery(cachedConnectionProvider.getConnection());

                int batchMaxRows = config.getInt(MessageSourceTaskConfig.BATCH_MAX_ROWS_CONFIG);
                boolean hadNext = true;
                while (results.size() < batchMaxRows && (hadNext = querier.next())) {
                    results.add(querier.extractRecord());
                }

                if (!hadNext) {
                    // If we finished processing the results from the current query, we can reset and send
                    // the querier to the tail of the queue
                    resetAndRequeueHead(querier, false);
                }

                if (results.isEmpty()) {
                    consecutiveEmptyResults.compute(querier, (k, v) -> v + 1);
                    log.info("No updates for {}", querier.toString());

                    if (Collections.min(consecutiveEmptyResults.values())
                            >= CONSECUTIVE_EMPTY_RESULTS_BEFORE_RETURN) {
                        log.info("More than " + CONSECUTIVE_EMPTY_RESULTS_BEFORE_RETURN
                                + " consecutive empty results for all queriers, returning");
                        return null;
                    } else {
                        continue;
                    }
                } else {
                    consecutiveEmptyResults.put(querier, 0);
                }

                log.debug("Returning {} records for {}", results.size(), querier);
                return results;
            } catch (SQLNonTransientException sqle) {
                log.error("Non-transient SQL exception while running query for table: {}",
                        querier, sqle);
                resetAndRequeueHead(querier, true);
                // This task has failed, so close any resources (may be reopened if needed) before throwing
                closeResources();
                throw new ConnectException(sqle);
            } catch (SQLException sqle) {
                log.error("SQL exception while running query for table: {}", querier, sqle);
                resetAndRequeueHead(querier, true);
                return null;
            } catch (Throwable t) {
                log.error("Failed to run query for table: {}", querier, t);
                resetAndRequeueHead(querier, true);
                // This task has failed, so close any resources (may be reopened if needed) before throwing
                closeResources();
                throw t;
            }
        }

        shutdown();
        return null;
    }

    @Override
    public void stop() {
        log.info("Message Source Task停止");

        // In earlier versions of Kafka, stop() was not called from the task thread. In this case, all
        // resources are closed at the end of 'poll()' when no longer running or if there is an error.
        running.set(false);

        if (taskThreadId.longValue() == Thread.currentThread().getId()) {
            shutdown();
        }
    }

    protected void closeResources() {
        log.info("Closing resources for JDBC source task");
        try {
            if (cachedConnectionProvider != null) {
                cachedConnectionProvider.close();
            }
        } catch (Throwable t) {
            log.warn("Error while closing the connections", t);
        } finally {
            cachedConnectionProvider = null;
            try {
                if (dialect != null) {
                    dialect.close();
                }
            } catch (Throwable t) {
                log.warn("Error while closing the {} dialect: ", dialect.name(), t);
            } finally {
                dialect = null;
            }
        }
    }

    private void shutdown() {
        final TableQuerier querier = tableQueue.peek();
        if (querier != null) {
            resetAndRequeueHead(querier, true);
        }
        closeResources();
    }

    private void resetAndRequeueHead(TableQuerier expectedHead, boolean resetOffset) {
        log.debug("Resetting querier {}", expectedHead.toString());
        TableQuerier removedQuerier = tableQueue.poll();
        assert removedQuerier == expectedHead;
        expectedHead.reset(time.milliseconds(), resetOffset);
        tableQueue.add(expectedHead);
    }


    private void validateNonNullable(
            String incrementalMode,
            String table,
            String incrementingColumn,
            List<String> timestampColumns
    ) {
        try {
            Set<String> lowercaseTsColumns = new HashSet<>();
            for (String timestampColumn : timestampColumns) {
                lowercaseTsColumns.add(timestampColumn.toLowerCase(Locale.getDefault()));
            }

            boolean incrementingOptional = false;
            boolean atLeastOneTimestampNotOptional = false;
            final Connection conn = cachedConnectionProvider.getConnection();
            boolean autoCommit = conn.getAutoCommit();
            try {
                conn.setAutoCommit(true);
                Map<ColumnId, ColumnDefinition> defnsById = dialect.describeColumns(conn, table, null);
                for (ColumnDefinition defn : defnsById.values()) {
                    String columnName = defn.id().name();
                    if (columnName.equalsIgnoreCase(incrementingColumn)) {
                        incrementingOptional = defn.isOptional();
                    } else if (lowercaseTsColumns.contains(columnName.toLowerCase(Locale.getDefault()))) {
                        if (!defn.isOptional()) {
                            atLeastOneTimestampNotOptional = true;
                        }
                    }
                }
            } finally {
                conn.setAutoCommit(autoCommit);
            }

            // Validate that requested columns for offsets are NOT NULL. Currently this is only performed
            // for table-based copying because custom query mode doesn't allow this to be looked up
            // without a query or parsing the query since we don't have a table name.
            if ((incrementalMode.equals(MessageSourceConnectorConfig.MODE_INCREMENTING)
                    || incrementalMode.equals(MessageSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING))
                    && incrementingOptional) {
                throw new ConnectException("Cannot make incremental queries using incrementing column "
                        + incrementingColumn + " on " + table + " because this column "
                        + "is nullable.");
            }
            if ((incrementalMode.equals(MessageSourceConnectorConfig.MODE_TIMESTAMP)
                    || incrementalMode.equals(MessageSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING))
                    && !atLeastOneTimestampNotOptional) {
                throw new ConnectException("Cannot make incremental queries using timestamp columns "
                        + timestampColumns + " on " + table + " because all of these "
                        + "columns "
                        + "nullable.");
            }
        } catch (SQLException e) {
            throw new ConnectException("Failed trying to validate that columns used for offsets are NOT"
                    + " NULL", e);
        }
    }
}
