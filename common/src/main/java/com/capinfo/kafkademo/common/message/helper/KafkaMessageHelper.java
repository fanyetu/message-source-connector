package com.capinfo.kafkademo.common.message.helper;

import cn.hutool.core.util.StrUtil;
import cn.hutool.system.SystemUtil;
import com.capinfo.kafkademo.common.message.db.MessageRepository;
import com.capinfo.kafkademo.common.message.kafka.ConsumeCustomMessageListener;
import com.capinfo.kafkademo.common.message.kafka.ReceiveCustomMessageListener;
import com.capinfo.kafkademo.common.message.kafka.ResponseCustomMessageListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
@Component
public class KafkaMessageHelper extends AbstractMessageHelper implements MessageHelper {

    public static final String GROUP_SUFFIX = "_GROUP";

    @Resource
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Resource
    private KafkaListenerContainerFactory kafkaListenerContainerFactory;

    public KafkaMessageHelper(MessageRepository messageRepository) {
        this.setMessageRepository(messageRepository);
    }

    @Override
    public RespMessage invoke(ReqMessage req) {
        return null;
    }

    @Override
    public void startReceive(String topic, MessageReceiveHandler handler) {
        ReceiveCustomMessageListener receiveCustomMessageListener = new ReceiveCustomMessageListener(handler, this);
        kafkaListenerEndpointRegistry.registerListenerContainer(
                receiveCustomMessageListener.createKafkaListenerEndpoint(StrUtil.EMPTY, topic,
                        SystemUtil.getHostInfo().getName() + Thread.currentThread().getName()),
                kafkaListenerContainerFactory, true);
    }

    @Override
    public void startResponse(String topic, MessageResponseHandler handler) {
        ResponseCustomMessageListener responseCustomMessageListener = new ResponseCustomMessageListener(handler, this);
        kafkaListenerEndpointRegistry.registerListenerContainer(
                responseCustomMessageListener.createKafkaListenerEndpoint(StrUtil.EMPTY, topic, topic + GROUP_SUFFIX),
                kafkaListenerContainerFactory, true);
    }


    @Override
    public void startConsume(String topic, MessageConsumeHandler handler) {
        ConsumeCustomMessageListener consumeCustomMessageListener = new ConsumeCustomMessageListener(handler, this);
        kafkaListenerEndpointRegistry.registerListenerContainer(
                consumeCustomMessageListener.createKafkaListenerEndpoint(StrUtil.EMPTY, topic,
                        SystemUtil.getHostInfo().getName() + Thread.currentThread().getName()),
                kafkaListenerContainerFactory, true);
    }
}
