package com.capinfo.kafkademo.common.message.helper;

import cn.hutool.core.util.StrUtil;
import com.capinfo.kafkademo.common.message.db.MessageRepository;
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

    }
}
