package com.capinfo.kafkademo.common.message.helper;

import cn.hutool.core.util.IdUtil;
import com.capinfo.kafkademo.common.message.db.Message;
import com.capinfo.kafkademo.common.message.db.MessageRepository;
import org.springframework.beans.BeanUtils;

import java.util.Date;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
public abstract class AbstractMessageHelper implements MessageHelper {

    private MessageRepository messageRepository;

    /**
     * 生成message id
     * @return
     */
    protected String genMessageId() {
        return IdUtil.fastSimpleUUID();
    }

    /**
     * 发送消息，存入数据库，MessageSourceConnector会将数据推送到kafka
     * @param req
     */
    @Override
    public void send(ReqMessage req) {
        sendMessage(req);
    }

    @Override
    public void response(RespMessage resp) {
        sendMessage(resp);
    }

    @Override
    public void publish(EventMessage event) {
        sendMessage(event);
    }

    private void sendMessage(BaseMessage req) {
        Message message = new Message();
        BeanUtils.copyProperties(req, message);
        message.setMessageId(genMessageId());
        message.setCreateTime(new Date());
        this.messageRepository.save(message);
    }

    public MessageRepository getMessageRepository() {
        return messageRepository;
    }

    public void setMessageRepository(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }
}
