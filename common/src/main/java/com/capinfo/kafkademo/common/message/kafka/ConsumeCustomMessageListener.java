package com.capinfo.kafkademo.common.message.kafka;

import com.capinfo.kafkademo.common.message.db.ReceivedMessage;
import com.capinfo.kafkademo.common.message.db.ReceivedMessageRepository;
import com.capinfo.kafkademo.common.message.helper.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.network.Receive;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Date;

/**
 * @author zhanghaonan
 * @date 2022/2/11
 */
public class ConsumeCustomMessageListener extends CustomMessageListener {

    private MessageConsumeHandler messageConsumeHandler;

    private KafkaMessageHelper kafkaMessageHelper;

    private ReceivedMessageRepository receivedMessageRepository;

    public ConsumeCustomMessageListener(MessageConsumeHandler messageConsumeHandler, KafkaMessageHelper kafkaMessageHelper,
                                        ReceivedMessageRepository receivedMessageRepository) {
        this.messageConsumeHandler = messageConsumeHandler;
        this.kafkaMessageHelper = kafkaMessageHelper;
        this.receivedMessageRepository = receivedMessageRepository;
    }

    @Override
    @SneakyThrows
    public KafkaListenerEndpoint createKafkaListenerEndpoint(String name, String topic, String groupId) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint =
                createDefaultMethodKafkaListenerEndpoint(name, topic, groupId);
        kafkaListenerEndpoint.setBean(new ConsumeMessageListener(messageConsumeHandler, kafkaMessageHelper,
                receivedMessageRepository));
        kafkaListenerEndpoint.setMethod(ConsumeMessageListener.class.getMethod(
                "onMessage", ConsumerRecord.class, Acknowledgment.class));
        return kafkaListenerEndpoint;
    }

    @Slf4j
    private static class ConsumeMessageListener implements AcknowledgingMessageListener<String, GenericData.Record> {

        private MessageConsumeHandler messageConsumeHandler;

        private KafkaMessageHelper kafkaMessageHelper;

        private ReceivedMessageRepository receivedMessageRepository;

        public ConsumeMessageListener(MessageConsumeHandler messageConsumeHandler, KafkaMessageHelper kafkaMessageHelper,
                                      ReceivedMessageRepository receivedMessageRepository) {
            this.messageConsumeHandler = messageConsumeHandler;
            this.kafkaMessageHelper = kafkaMessageHelper;
            this.receivedMessageRepository = receivedMessageRepository;
        }

        @Override
        public void onMessage(ConsumerRecord<String, GenericData.Record> record, Acknowledgment acknowledgment) {
            log.info("Consume message listener 开始处理消息记录: " + record);
            GenericData.Record value = record.value();

            EventMessage event = EventMessage.avroToMessage(value);

            ReceivedMessage receivedMessage = new ReceivedMessage();
            receivedMessage.setMessageId(event.getMessageId());
            receivedMessage.setReceiveTime(new Date());
            receivedMessage.setTopic(event.getSourceTopic());
            receivedMessage.setContent(event.getContent());
            receivedMessage.setInstanceKey(kafkaMessageHelper.getInstanceKey());
            receivedMessageRepository.save(receivedMessage);

            messageConsumeHandler.consume(event);
            // 提交offset
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
            log.info("Consume message listener 消息记录处理完成");
        }
    }
}
