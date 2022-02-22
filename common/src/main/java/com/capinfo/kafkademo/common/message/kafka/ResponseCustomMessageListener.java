package com.capinfo.kafkademo.common.message.kafka;

import com.capinfo.kafkademo.common.message.db.ReceivedMessage;
import com.capinfo.kafkademo.common.message.db.ReceivedMessageRepository;
import com.capinfo.kafkademo.common.message.helper.KafkaMessageHelper;
import com.capinfo.kafkademo.common.message.helper.MessageResponseHandler;
import com.capinfo.kafkademo.common.message.helper.ReqMessage;
import com.capinfo.kafkademo.common.message.helper.RespMessage;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.lang.reflect.Method;
import java.util.Date;

/**
 * @author zhanghaonan
 * @date 2022/2/10
 */
public class ResponseCustomMessageListener extends CustomMessageListener {

    private MessageResponseHandler messageResponseHandler;

    private KafkaMessageHelper kafkaMessageHelper;

    private ReceivedMessageRepository receivedMessageRepository;

    public ResponseCustomMessageListener(MessageResponseHandler messageResponseHandler, KafkaMessageHelper kafkaMessageHelper,
                                         ReceivedMessageRepository receivedMessageRepository) {
        this.messageResponseHandler = messageResponseHandler;
        this.kafkaMessageHelper = kafkaMessageHelper;
        this.receivedMessageRepository = receivedMessageRepository;
    }

    @Override
    @SneakyThrows
    public KafkaListenerEndpoint createKafkaListenerEndpoint(String name, String topic, String groupId) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint =
                createDefaultMethodKafkaListenerEndpoint(name, topic, groupId);
        kafkaListenerEndpoint.setBean(new ResponseMessageListener(messageResponseHandler, kafkaMessageHelper,
                receivedMessageRepository));
        kafkaListenerEndpoint.setMethod(ResponseMessageListener.class.getMethod(
                "onMessage", ConsumerRecord.class, Acknowledgment.class));
        return kafkaListenerEndpoint;
    }

    @Slf4j
    private static class ResponseMessageListener implements AcknowledgingMessageListener<String, GenericData.Record> {

        private MessageResponseHandler messageResponseHandler;

        private KafkaMessageHelper kafkaMessageHelper;


        private ReceivedMessageRepository receivedMessageRepository;

        public ResponseMessageListener(MessageResponseHandler messageResponseHandler, KafkaMessageHelper kafkaMessageHelper,
                                       ReceivedMessageRepository receivedMessageRepository) {
            this.messageResponseHandler = messageResponseHandler;
            this.kafkaMessageHelper = kafkaMessageHelper;
            this.receivedMessageRepository = receivedMessageRepository;
        }

        @Override
        public void onMessage(ConsumerRecord<String, GenericData.Record> record, Acknowledgment acknowledgment) {
            log.info("Response message listener 开始处理消息记录: " + record);

            // 读取数据，并返回
            GenericData.Record value = record.value();
            ReqMessage req = ReqMessage.avroToMessage(value);

            ReceivedMessage receivedMessage = new ReceivedMessage();
            receivedMessage.setMessageId(req.getMessageId());
            receivedMessage.setTopic(req.getTargetTopic());
            receivedMessage.setReceiveTime(new Date());
            receivedMessage.setContent(req.getContent());
            receivedMessage.setInstanceKey(kafkaMessageHelper.getInstanceKey());
            receivedMessageRepository.save(receivedMessage);

            RespMessage resp = new RespMessage();
            resp.setTargetTopic(req.getSourceTopic());
            resp.setSourceTopic(req.getTargetTopic());
            resp.setMessageId(req.getMessageId());

            resp = messageResponseHandler.receive(req, resp);
            kafkaMessageHelper.response(resp);
            // 提交offset
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }

            log.info("Response message listener 消息记录处理完成. 返回值: " + resp);
        }
    }

    public static void main(String[] args) throws NoSuchMethodException {
        Method[] methods = ResponseMessageListener.class.getMethods();
        Method method = ResponseMessageListener.class.getMethod("onMessage", ConsumerRecord.class, Acknowledgment.class);
        System.out.println(methods);
    }
}
