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
        /*
        1.调用send方法发送消息；
        2.将请求消息存入内存；
        3.判断是否已有接收线程；
        4.如果有，则使用现有的接收线程；
        5.如果没有，则启动接收线程；
        6.主线程等待；
        7.接收到数据后判断当前内存中是否有该消息；
        8.如果有该消息，则将返回消息设置在内存中，并唤醒主线程；
        9.如果没有该消息，则跳过；
        10.主线程通过messageId获取返回消息，删除内存中消息，并返回。
         */

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
