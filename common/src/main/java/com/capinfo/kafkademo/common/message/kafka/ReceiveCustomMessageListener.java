package com.capinfo.kafkademo.common.message.kafka;

import com.capinfo.kafkademo.common.message.db.Message;
import com.capinfo.kafkademo.common.message.db.ReceivedMessage;
import com.capinfo.kafkademo.common.message.db.ReceivedMessageRepository;
import com.capinfo.kafkademo.common.message.helper.KafkaMessageHelper;
import com.capinfo.kafkademo.common.message.helper.MessageReceiveHandler;
import com.capinfo.kafkademo.common.message.helper.ReqMessage;
import com.capinfo.kafkademo.common.message.helper.RespMessage;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.BeanUtils;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.listener.MessageListener;

import java.util.Date;

/**
 * @author zhanghaonan
 * @date 2022/2/11
 */
public class ReceiveCustomMessageListener extends CustomMessageListener {

    private MessageReceiveHandler messageReceiveHandler;

    private KafkaMessageHelper kafkaMessageHelper;

    private ReceivedMessageRepository receivedMessageRepository;

    public ReceiveCustomMessageListener(MessageReceiveHandler messageReceiveHandler, KafkaMessageHelper kafkaMessageHelper,
                                        ReceivedMessageRepository receivedMessageRepository) {
        this.messageReceiveHandler = messageReceiveHandler;
        this.kafkaMessageHelper = kafkaMessageHelper;
        this.receivedMessageRepository = receivedMessageRepository;
    }

    @Override
    @SneakyThrows
    public KafkaListenerEndpoint createKafkaListenerEndpoint(String name, String topic, String groupId) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint =
                createDefaultMethodKafkaListenerEndpoint(name, topic, groupId);
        kafkaListenerEndpoint.setBean(new ReceiveCustomMessageListener.ReceiveMessageListener(messageReceiveHandler,
                kafkaMessageHelper, receivedMessageRepository));
        kafkaListenerEndpoint.setMethod(ReceiveCustomMessageListener.ReceiveMessageListener.class.getMethod(
                "onMessage", ConsumerRecord.class));
        return kafkaListenerEndpoint;
    }

    @Slf4j
    private static class ReceiveMessageListener implements MessageListener<String, GenericData.Record> {

        private MessageReceiveHandler messageReceiveHandler;

        private KafkaMessageHelper kafkaMessageHelper;

        private ReceivedMessageRepository receivedMessageRepository;

        public ReceiveMessageListener(MessageReceiveHandler messageReceiveHandler, KafkaMessageHelper kafkaMessageHelper,
                                      ReceivedMessageRepository receivedMessageRepository) {
            this.messageReceiveHandler = messageReceiveHandler;
            this.kafkaMessageHelper = kafkaMessageHelper;
            this.receivedMessageRepository = receivedMessageRepository;
        }

        @Override
        public void onMessage(ConsumerRecord<String, GenericData.Record> record) {
            log.info("Receive message listener 开始处理消息记录: " + record);
            // 读取数据，判断是否是在当前内存中，如果在则处理。
            GenericData.Record value = record.value();
            String messageId = ((Utf8) value.get("message_id")).toString();
            Message message = kafkaMessageHelper.getUnprocessedReqMessage(messageId);
            if (message == null || !kafkaMessageHelper.getInstanceKey().equals(message.getInstanceKey())) {
                log.info("Receive message listener 没有找到该消息对应的请求消息，跳过处理");
                return;
            }

            ReqMessage reqMessage = new ReqMessage();
            BeanUtils.copyProperties(message, reqMessage);

            RespMessage resp = RespMessage.avroToMessage(value);
            ReceivedMessage receivedMessage = new ReceivedMessage();
            receivedMessage.setMessageId(resp.getMessageId());
            receivedMessage.setReceiveTime(new Date());
            receivedMessage.setTopic(resp.getTargetTopic());
            receivedMessage.setContent(resp.getContent());
            receivedMessage.setInstanceKey(kafkaMessageHelper.getInstanceKey());
            receivedMessageRepository.save(receivedMessage);

            messageReceiveHandler.receive(reqMessage, resp);
            kafkaMessageHelper.makeReqMessageProcessed(messageId);
            log.info("Receive message listener 消息记录处理完成");
        }
    }
}
