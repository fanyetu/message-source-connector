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
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

/**
 * @author zhanghaonan
 * @date 2022/2/11
 */
public class ReceiveCustomMessageListener extends CustomMessageListener {

    private MessageReceiveHandler messageReceiveHandler;

    private KafkaMessageHelper kafkaMessageHelper;

    private ReceivedMessageRepository receivedMessageRepository;

    private final ConcurrentHashMap<String, Thread> threadMap;

    private final ConcurrentHashMap<String, RespMessage> respMap;

    public ReceiveCustomMessageListener(MessageReceiveHandler messageReceiveHandler, KafkaMessageHelper kafkaMessageHelper,
                                        ReceivedMessageRepository receivedMessageRepository, ConcurrentHashMap<String, Thread> threadMap,
                                        ConcurrentHashMap<String, RespMessage> respMap) {
        this.messageReceiveHandler = messageReceiveHandler;
        this.kafkaMessageHelper = kafkaMessageHelper;
        this.receivedMessageRepository = receivedMessageRepository;
        this.threadMap = threadMap;
        this.respMap = respMap;
    }

    @Override
    @SneakyThrows
    public KafkaListenerEndpoint createKafkaListenerEndpoint(String name, String topic, String groupId) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint =
                createDefaultMethodKafkaListenerEndpoint(name, topic, groupId);
        kafkaListenerEndpoint.setBean(new ReceiveCustomMessageListener.ReceiveMessageListener(messageReceiveHandler,
                kafkaMessageHelper, receivedMessageRepository, threadMap, respMap));
        kafkaListenerEndpoint.setMethod(ReceiveCustomMessageListener.ReceiveMessageListener.class.getMethod(
                "onMessage", ConsumerRecord.class, Acknowledgment.class));
        return kafkaListenerEndpoint;
    }

    @Slf4j
    private static class ReceiveMessageListener implements AcknowledgingMessageListener<String, GenericData.Record> {

        private MessageReceiveHandler messageReceiveHandler;

        private KafkaMessageHelper kafkaMessageHelper;

        private ReceivedMessageRepository receivedMessageRepository;

        private final ConcurrentHashMap<String, Thread> threadMap;

        private final ConcurrentHashMap<String, RespMessage> respMap;

        public ReceiveMessageListener(MessageReceiveHandler messageReceiveHandler, KafkaMessageHelper kafkaMessageHelper,
                                      ReceivedMessageRepository receivedMessageRepository, ConcurrentHashMap<String, Thread> threadMap,
                                      ConcurrentHashMap<String, RespMessage> respMap) {
            this.messageReceiveHandler = messageReceiveHandler;
            this.kafkaMessageHelper = kafkaMessageHelper;
            this.receivedMessageRepository = receivedMessageRepository;
            this.threadMap = threadMap;
            this.respMap = respMap;
        }

        @Override
        public void onMessage(ConsumerRecord<String, GenericData.Record> record, Acknowledgment acknowledgment) {
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
            // TODO 重启kafka之后会重新消费，存储时会报主键重复
            receivedMessageRepository.save(receivedMessage);

            respMap.put(messageId, resp);

            // 判断是否有线程在等待
            Thread mainThread = threadMap.get(messageId);
            if (mainThread != null) {
                LockSupport.unpark(mainThread);
            }

            messageReceiveHandler.receive(reqMessage, resp);
            kafkaMessageHelper.makeReqMessageProcessed(messageId);
            // 提交offset
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
            log.info("Receive message listener 消息记录处理完成");
        }
    }
}
