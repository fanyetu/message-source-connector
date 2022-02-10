package com.capinfo.kafkademo.common.message.helper;

import com.capinfo.kafkademo.common.message.db.MessageRepository;
import org.springframework.stereotype.Component;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
@Component
public class KafkaMessageHelper extends AbstractMessageHelper implements MessageHelper{

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
    public void startConsume(String topic, MessageConsumeHandler handler) {

    }
}
