package com.capinfo.kafkademo.common.message.kafka;

import com.capinfo.kafkademo.common.message.helper.KafkaMessageHelper;
import com.capinfo.kafkademo.common.message.helper.MessageReceiveHandler;
import com.capinfo.kafkademo.common.message.helper.ReqMessage;
import com.capinfo.kafkademo.common.message.helper.RespMessage;
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
public class ReceiveCustomMessageListener extends CustomMessageListener {

    private MessageReceiveHandler messageReceiveHandler;

    private KafkaMessageHelper kafkaMessageHelper;

    public ReceiveCustomMessageListener(MessageReceiveHandler messageReceiveHandler, KafkaMessageHelper kafkaMessageHelper) {
        this.messageReceiveHandler = messageReceiveHandler;
        this.kafkaMessageHelper = kafkaMessageHelper;
    }

    @Override
    @SneakyThrows
    public KafkaListenerEndpoint createKafkaListenerEndpoint(String name, String topic, String groupId) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint =
                createDefaultMethodKafkaListenerEndpoint(name, topic, groupId);
        kafkaListenerEndpoint.setBean(new ReceiveCustomMessageListener.ReceiveMessageListener(messageReceiveHandler, kafkaMessageHelper));
        kafkaListenerEndpoint.setMethod(ReceiveCustomMessageListener.ReceiveMessageListener.class.getMethod(
                "onMessage", ConsumerRecord.class));
        return kafkaListenerEndpoint;
    }

    @Slf4j
    private static class ReceiveMessageListener implements MessageListener<String, GenericData.Record> {

        private MessageReceiveHandler messageReceiveHandler;

        private KafkaMessageHelper kafkaMessageHelper;

        public ReceiveMessageListener(MessageReceiveHandler messageReceiveHandler, KafkaMessageHelper kafkaMessageHelper) {
            this.messageReceiveHandler = messageReceiveHandler;
            this.kafkaMessageHelper = kafkaMessageHelper;
        }

        @Override
        public void onMessage(ConsumerRecord<String, GenericData.Record> record) {
            log.info("Receive message listener 开始处理消息记录: " + record);
            // 读取数据，判断是否是在当前内存中，如果在则处理。
            GenericData.Record value = record.value();
            String messageId = ((Utf8) value.get("message_id")).toString();
            ReqMessage reqMessage = kafkaMessageHelper.getReqMessage(messageId);
            if (reqMessage == null) {
                log.info("Receive message listener 没有找到该消息对应的请求消息，跳过处理");
                return;
            }

            RespMessage resp = RespMessage.avroToMessage(value);
            messageReceiveHandler.receive(reqMessage, resp);
            kafkaMessageHelper.removeReqMessage(messageId);
            log.info("Receive message listener 消息记录处理完成");
        }
    }
}
