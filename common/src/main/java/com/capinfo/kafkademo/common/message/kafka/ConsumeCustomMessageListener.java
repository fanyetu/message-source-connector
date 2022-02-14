package com.capinfo.kafkademo.common.message.kafka;

import com.capinfo.kafkademo.common.message.helper.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.listener.MessageListener;

/**
 * @author zhanghaonan
 * @date 2022/2/11
 */
public class ConsumeCustomMessageListener extends CustomMessageListener {

    private MessageConsumeHandler messageConsumeHandler;

    private KafkaMessageHelper kafkaMessageHelper;

    public ConsumeCustomMessageListener(MessageConsumeHandler messageConsumeHandler, KafkaMessageHelper kafkaMessageHelper) {
        this.messageConsumeHandler = messageConsumeHandler;
        this.kafkaMessageHelper = kafkaMessageHelper;
    }

    @Override
    @SneakyThrows
    public KafkaListenerEndpoint createKafkaListenerEndpoint(String name, String topic, String groupId) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint =
                createDefaultMethodKafkaListenerEndpoint(name, topic, groupId);
        kafkaListenerEndpoint.setBean(new ConsumeMessageListener(messageConsumeHandler, kafkaMessageHelper));
        kafkaListenerEndpoint.setMethod(ConsumeMessageListener.class.getMethod(
                "onMessage", ConsumerRecord.class));
        return kafkaListenerEndpoint;
    }

    @Slf4j
    private static class ConsumeMessageListener implements MessageListener<String, GenericData.Record> {

        private MessageConsumeHandler messageConsumeHandler;

        private KafkaMessageHelper kafkaMessageHelper;

        public ConsumeMessageListener(MessageConsumeHandler messageConsumeHandler, KafkaMessageHelper kafkaMessageHelper) {
            this.messageConsumeHandler = messageConsumeHandler;
            this.kafkaMessageHelper = kafkaMessageHelper;
        }

        @Override
        public void onMessage(ConsumerRecord<String, GenericData.Record> record) {
            log.info("Consume message listener 开始处理消息记录: " + record);
            // 读取数据，判断是否是在当前内存中，如果在则处理。
            GenericData.Record value = record.value();

            EventMessage event = EventMessage.avroToMessage(value);
            messageConsumeHandler.consume(event);
            log.info("Consume message listener 消息记录处理完成");
        }
    }
}
