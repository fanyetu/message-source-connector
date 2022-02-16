package com.capinfo.kafkademo.common.message.helper;

import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.system.SystemUtil;
import com.capinfo.kafkademo.common.message.constant.MessageConstant;
import com.capinfo.kafkademo.common.message.db.Message;
import com.capinfo.kafkademo.common.message.db.MessageRepository;
import com.capinfo.kafkademo.common.message.db.ReceivedMessage;
import com.capinfo.kafkademo.common.message.db.ReceivedMessageRepository;
import com.capinfo.kafkademo.common.message.kafka.ConsumeCustomMessageListener;
import com.capinfo.kafkademo.common.message.kafka.ReceiveCustomMessageListener;
import com.capinfo.kafkademo.common.message.kafka.ResponseCustomMessageListener;
import org.apache.kafka.common.network.Receive;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TODO 加上数据校验
 *
 * @author zhanghaonan
 * @date 2022/2/5
 */
@Component
public class KafkaMessageHelper implements MessageHelper {

    public static final String GROUP_SUFFIX = "_GROUP";

    @Resource
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Resource
    private KafkaListenerContainerFactory kafkaListenerContainerFactory;

    @Resource
    private MessageRepository messageRepository;

    @Resource
    private ReceivedMessageRepository receivedMessageRepository;

    @Value("${server.port}")
    private String port;

    private final ConcurrentHashMap<String, String> listenerMap = new ConcurrentHashMap<>(8);

    /**
     * 删除对应的reqMessage
     *
     * @param messageId
     */
    public void makeReqMessageProcessed(String messageId) {
        messageRepository.updateProcessFlag(messageId);
    }

    /**
     * 通过messageId和processFlag获取请求消息
     *
     * @param messageId
     * @return
     */
    public Message getUnprocessedReqMessage(String messageId) {
        return messageRepository.findMessageByMessageIdAndProcessFlag(messageId, MessageConstant.FALSE);
    }

    /**
     * 生成message id
     *
     * @return
     */
    protected String genMessageId() {
        return IdUtil.fastSimpleUUID();
    }

    /**
     * 发送消息，存入数据库，MessageSourceConnector会将数据推送到kafka
     *
     * @param req
     */
    @Override
    public String send(ReqMessage req) {
        String messageId = genMessageId();
        req.setMessageId(messageId);
        sendMessage(req, MessageConstant.MESSAGE_TYPE_REQ);
        return messageId;
    }

    /**
     * 返回响应消息
     *
     * @param resp
     */
    @Override
    public void response(RespMessage resp) {
        sendMessage(resp, MessageConstant.MESSAGE_TYPE_RESP);
    }

    @Override
    public String publish(EventMessage event) {
        String messageId = genMessageId();
        event.setMessageId(messageId);
        sendMessage(event, MessageConstant.MESSAGE_TYPE_EVENT);
        return messageId;
    }

    private void sendMessage(BaseMessage req, String messageType) {
        Message message = new Message();
        BeanUtils.copyProperties(req, message);
        message.setCreateTime(new Date());
        message.setInstanceKey(getInstanceKey());
        message.setProcessFlag(
                MessageConstant.MESSAGE_TYPE_REQ.equals(messageType) ?
                        MessageConstant.FALSE : MessageConstant.TRUE
        );
        message.setMessageType(messageType);
        this.messageRepository.save(message);
    }

    @Override
    public String getInstanceKey() {
        return SystemUtil.getHostInfo().getAddress() + ":" + port;
    }

    @Override
    public RespMessage invoke(ReqMessage req) {
        String messageId = send(req);

        String sourceTopic = req.getSourceTopic();
        String s = listenerMap.get(sourceTopic);
        if (StrUtil.isBlank(s)) {
            startReceive(sourceTopic, (request, response) -> {
                // do nothing
            });
        }

        int count = 300;
        while (true) {
            ReceivedMessage result = receivedMessageRepository.findReceivedMessageByMessageId(messageId);
            if (result != null) {
                RespMessage respMessage = new RespMessage();
                respMessage.setMessageId(messageId);
                respMessage.setSourceTopic(req.getTargetTopic());
                respMessage.setTargetTopic(result.getTopic());
                respMessage.setContent(result.getContent());
                return respMessage;
            }

            if (count <= 0) {
                return null;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // do nothing
            }
            count--;
        }
    }

    @Override
    public void startReceive(String topic, MessageReceiveHandler handler) {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        try {
            checkListener(topic);
            ReceiveCustomMessageListener receiveCustomMessageListener = new ReceiveCustomMessageListener(handler,
                    this, receivedMessageRepository);
            kafkaListenerEndpointRegistry.registerListenerContainer(
                    receiveCustomMessageListener.createKafkaListenerEndpoint(StrUtil.EMPTY, topic,
                            getInstanceKey() + GROUP_SUFFIX),
                    kafkaListenerContainerFactory, true);
            listenerMap.put(topic, MessageConstant.TRUE);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void startResponse(String topic, MessageResponseHandler handler) {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        try {
            checkListener(topic);
            ResponseCustomMessageListener responseCustomMessageListener = new ResponseCustomMessageListener(handler,
                    this, receivedMessageRepository);
            kafkaListenerEndpointRegistry.registerListenerContainer(
                    responseCustomMessageListener.createKafkaListenerEndpoint(StrUtil.EMPTY, topic, topic + GROUP_SUFFIX),
                    kafkaListenerContainerFactory, true);
            listenerMap.put(topic, MessageConstant.TRUE);
        } finally {
            lock.unlock();
        }
    }


    @Override
    public void startConsume(String topic, MessageConsumeHandler handler) {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        try {
            checkListener(topic);
            ConsumeCustomMessageListener consumeCustomMessageListener = new ConsumeCustomMessageListener(handler,
                    this, receivedMessageRepository);
            kafkaListenerEndpointRegistry.registerListenerContainer(
                    consumeCustomMessageListener.createKafkaListenerEndpoint(StrUtil.EMPTY, topic,
                            topic + GROUP_SUFFIX),
                    kafkaListenerContainerFactory, true);
            listenerMap.put(topic, MessageConstant.TRUE);
        } finally {
            lock.unlock();
        }
    }

    private void checkListener(String topic) {
        String s = listenerMap.get(topic);
        if (StrUtil.isNotBlank(s)) {
            throw new RuntimeException("当前已注册该主题监听器，无需重复注册");
        }
    }
}
