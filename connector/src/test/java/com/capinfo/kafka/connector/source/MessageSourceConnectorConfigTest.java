package com.capinfo.kafka.connector.source;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhanghaonan
 * @date 2022/2/9
 */
public class MessageSourceConnectorConfigTest {

    @Test
    public void testGet() {
        Map<String, String > properties = new HashMap<>();
        properties.put("connection.url","jdbc:mysql://192.168.152.128:3306/test");
        properties.put("connection.user", "root");
        properties.put("connection.password", "111111");
        properties.put("query", "select id,target,source,data,create_time from t_message");
        properties.put("mode", "timestamp+incrementing");
        properties.put("timestamp.column.name", "create_time");
        properties.put("incrementing.column.name", "id");
        properties.put("source", "source");
        properties.put("target", "target");
        MessageSourceConnectorConfig config = new MessageSourceConnectorConfig(properties);
        String quoteSql = config.getString(MessageSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_CONFIG);

        Assert.assertNotNull(quoteSql);

    }
}
