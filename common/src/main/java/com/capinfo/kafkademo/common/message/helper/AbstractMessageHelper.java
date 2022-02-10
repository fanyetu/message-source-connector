package com.capinfo.kafkademo.common.message.helper;

import com.capinfo.kafkademo.common.message.db.Message;
import com.capinfo.kafkademo.common.message.db.MessageRepository;
import org.springframework.beans.BeanUtils;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
public abstract class AbstractMessageHelper implements MessageHelper {

    private MessageRepository messageRepository;

    /**
     * 发送消息，存入数据库，MessageSourceConnector会将数据推送到kafka
     * @param req
     * @return
     */
    @Override
    public boolean send(ReqMessage req) {
        Message message = new Message();
        BeanUtils.copyProperties(req, message);
        this.messageRepository.save(message);
        return true;
    }

    @Override
    public boolean publish(EventMessage event) {
        Message message = new Message();
        BeanUtils.copyProperties(event, message);
        this.messageRepository.save(message);
        return true;
    }

    public MessageRepository getMessageRepository() {
        return messageRepository;
    }

    public void setMessageRepository(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }
}
