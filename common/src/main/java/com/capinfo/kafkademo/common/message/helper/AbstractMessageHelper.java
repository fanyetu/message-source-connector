package com.capinfo.kafkademo.common.message.helper;

import cn.hutool.core.util.IdUtil;
import com.capinfo.kafkademo.common.message.db.Message;
import com.capinfo.kafkademo.common.message.db.MessageRepository;
import org.springframework.beans.BeanUtils;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
public abstract class AbstractMessageHelper implements MessageHelper {

    private MessageRepository messageRepository;

    protected ConcurrentHashMap<String, Message> reqMessageConcurrentHashMap
            = new ConcurrentHashMap<>(16);

    /**
     * 删除对应的reqMessage
     * @param messageId
     */
    public void removeReqMessage(String messageId) {
        reqMessageConcurrentHashMap.remove(messageId);
    }

    /**
     * 通过messageId获取请求消息
     *
     * @param messageId
     * @return
     */
    public ReqMessage getReqMessage(String messageId) {
        Message message = reqMessageConcurrentHashMap.get(messageId);
        if (message == null) {
            return null;
        }
        ReqMessage reqMessage = new ReqMessage();
        reqMessage.setMessageId(message.getMessageId());
        reqMessage.setSourceTopic(message.getSourceTopic());
        reqMessage.setTargetTopic(message.getTargetTopic());
        reqMessage.setContent(message.getContent());

        return reqMessage;
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
    public void send(ReqMessage req) {
        Message message = sendMessage(req);
        // 存入内存中
        reqMessageConcurrentHashMap.put(message.getMessageId(), message);
    }

    @Override
    public void response(RespMessage resp) {
        sendMessage(resp);
    }

    @Override
    public void publish(EventMessage event) {
        sendMessage(event);
    }

    private Message sendMessage(BaseMessage req) {
        Message message = new Message();
        BeanUtils.copyProperties(req, message);
        message.setMessageId(genMessageId());
        message.setCreateTime(new Date());
        this.messageRepository.save(message);
        return message;
    }

    public MessageRepository getMessageRepository() {
        return messageRepository;
    }

    public void setMessageRepository(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }
}
