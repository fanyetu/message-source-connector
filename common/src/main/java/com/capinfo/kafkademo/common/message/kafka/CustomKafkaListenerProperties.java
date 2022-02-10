package com.capinfo.kafkademo.common.message.kafka;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * @author zhanghaonan
 * @date 2022/2/10
 */
@Data
@ConfigurationProperties(prefix = "custom.kafka")
public class CustomKafkaListenerProperties {
    private Map<String, CustomKafkaListenerProperty> listeners;
}
