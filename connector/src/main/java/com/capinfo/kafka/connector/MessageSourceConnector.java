package com.capinfo.kafka.connector;

import com.capinfo.kafka.connector.dialect.DatabaseDialect;
import com.capinfo.kafka.connector.dialect.DatabaseDialects;
import com.capinfo.kafka.connector.source.MessageSourceConnectorConfig;
import com.capinfo.kafka.connector.source.MessageSourceTask;
import com.capinfo.kafka.connector.util.CachedConnectionProvider;
import com.capinfo.kafka.connector.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author zhanghaonan
 * @date 2022/2/7
 */
public class MessageSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MessageSourceConnector.class);

    private Map<String, String> configProperties;
    private MessageSourceConnectorConfig config;
    private DatabaseDialect dialect;
    private CachedConnectionProvider cachedConnectionProvider;

    /**
     * Connector生命周期方法，启动connector
     *
     * @param properties
     */
    @Override
    public void start(Map<String, String> properties) {
        log.info("启动Message source connector");
        try {
            configProperties = properties;
            config = new MessageSourceConnectorConfig(configProperties);
        } catch (ConfigException e) {
            throw new ConnectException("配置加载失败，无法启动MessageSourceConnector", e);
        }

        final String dbUrl = config.getString(MessageSourceConnectorConfig.CONNECTION_URL_CONFIG);
        final int maxConnectionAttempts = config.getInt(MessageSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
        final long connectionRetryBackoff = config.getLong(MessageSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);

        // 查找最合适的数据库实现
        dialect = DatabaseDialects.findBestFor(
                dbUrl,
                config
        );

        // 初始化jdbc connection provider
        cachedConnectionProvider = connectionProvider(maxConnectionAttempts, connectionRetryBackoff);

        // 初次连接尝试
        cachedConnectionProvider.getConnection();
    }

    protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        return new CachedConnectionProvider(dialect, maxConnAttempts, retryBackoff);
    }

    /**
     * 定义了worker进程中应该实例化的类，以实际读取数据
     * 这里我们返回的就是{@link MessageSourceTask}类
     *
     * @return
     */
    @Override
    public Class<? extends Task> taskClass() {
        return MessageSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> taskProps = new HashMap<>(configProperties);
        List<Map<String, String>> taskConfigs = Collections.singletonList(taskProps);
        log.info("Task配置初始化");
        return taskConfigs;
    }

    private static final long MAX_TIMEOUT = 10000L;

    /**
     * connector生命周期方法，停止connector
     */
    @Override
    public void stop() {
        try {
            cachedConnectionProvider.close();
        } finally {
            try {
                if (dialect != null) {
                    dialect.close();
                }
            } catch (Throwable t) {
                log.warn("关闭{}数据库失败: ", dialect, t);
            } finally {
                dialect = null;
            }
        }
    }

    /**
     * 输出配置验证规则
     *
     * @return
     */
    @Override
    public ConfigDef config() {
        return MessageSourceConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
