package com.capinfo.kafka.connector.source;

import java.util.Map;

/**
 * @author zhanghaonan
 * @date 2022/2/8
 */
public class MessageSourceTaskConfig extends MessageSourceConnectorConfig {

    public MessageSourceTaskConfig(Map<String, String> configProperties) {
        super(configProperties);
    }
}
