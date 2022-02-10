package com.capinfo.kafkademo.common.message.kafka;

import cn.hutool.core.util.StrUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

import java.util.Optional;

/**
 * @author zhanghaonan
 * @date 2022/2/10
 */
public abstract class CustomMessageListener {

    private static int NUMBER_OF_LISTENERS = 0;

    @Autowired
    private KafkaProperties kafkaProperties;

    public abstract KafkaListenerEndpoint createKafkaListenerEndpoint(String name, String topic);

    protected MethodKafkaListenerEndpoint<String, String> createDefaultMethodKafkaListenerEndpoint(
            String name, String topic) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint =
                new MethodKafkaListenerEndpoint<>();
        kafkaListenerEndpoint.setId(getConsumerId(name));
        kafkaListenerEndpoint.setGroupId(kafkaProperties.getConsumer().getGroupId());
        kafkaListenerEndpoint.setAutoStartup(true);
        kafkaListenerEndpoint.setTopics(topic);
        kafkaListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());
        return kafkaListenerEndpoint;
    }

    private String getConsumerId(String name) {
        if (StrUtil.isBlank(name)) {
            return CustomMessageListener.class.getCanonicalName() + "#" + NUMBER_OF_LISTENERS++;
        } else {
            return name;
        }
    }
}
