package com.capinfo.kafkademo.common.message.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhanghaonan
 * @date 2022/2/10
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CustomKafkaListenerProperty {
    private String topic;
    private String listenerClass;
}
