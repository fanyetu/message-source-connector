package com.capinfo.kafkademo.common.message.kafka;

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
import org.springframework.kafka.listener.MessageListener;

/**
 * @author zhanghaonan
 * @date 2022/2/10
 */
public class ResponseCustomMessageListener extends CustomMessageListener{

    private MessageResponseHandler messageResponseHandler;

    private KafkaMessageHelper kafkaMessageHelper;

    public ResponseCustomMessageListener(MessageResponseHandler messageResponseHandler, KafkaMessageHelper kafkaMessageHelper) {
        this.messageResponseHandler = messageResponseHandler;
        this.kafkaMessageHelper = kafkaMessageHelper;
    }

    @Override
    @SneakyThrows
    public KafkaListenerEndpoint createKafkaListenerEndpoint(String name, String topic, String groupId) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint =
                createDefaultMethodKafkaListenerEndpoint(name, topic, groupId);
        kafkaListenerEndpoint.setBean(new ResponseMessageListener(messageResponseHandler, kafkaMessageHelper));
        kafkaListenerEndpoint.setMethod(ResponseMessageListener.class.getMethod(
                "onMessage", ConsumerRecord.class));
        return kafkaListenerEndpoint;
    }

    @Slf4j
    private static class ResponseMessageListener implements MessageListener<String, GenericData.Record> {

        private MessageResponseHandler messageResponseHandler;

        private KafkaMessageHelper kafkaMessageHelper;

        public ResponseMessageListener(MessageResponseHandler messageResponseHandler, KafkaMessageHelper kafkaMessageHelper) {
            this.messageResponseHandler = messageResponseHandler;
            this.kafkaMessageHelper = kafkaMessageHelper;
        }

        @Override
        public void onMessage(ConsumerRecord<String, GenericData.Record> record) {
            log.info("Response message listener 开始处理消息记录: " + record);

            // 读取数据，并返回
            GenericData.Record value = record.value();
            ReqMessage req = ReqMessage.avroToMessage(value);
            RespMessage resp = new RespMessage();

            // TODO 存储reqMessage
            resp.setTargetTopic(req.getSourceTopic());
            resp.setSourceTopic(req.getTargetTopic());
            resp.setMessageId(req.getMessageId());

            resp = messageResponseHandler.receive(req, resp);
            kafkaMessageHelper.response(resp);

            log.info("Response message listener 消息记录处理完成. 返回值: " + resp);
        }
    }
}
