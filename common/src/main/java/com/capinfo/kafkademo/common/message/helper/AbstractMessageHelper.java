package com.capinfo.kafkademo.common.message.helper;

import cn.hutool.core.util.IdUtil;
import com.capinfo.kafkademo.common.message.db.Message;
import com.capinfo.kafkademo.common.message.db.MessageRepository;
import org.springframework.beans.BeanUtils;
import reactor.util.function.Tuple3;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
public abstract class AbstractMessageHelper implements MessageHelper {

    private MessageRepository messageRepository;

    protected ConcurrentHashMap<String, Tuple3<Thread, ReqMessage, RespMessage>> reqMessageConcurrentHashMap
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
        Tuple3<Thread, ReqMessage, RespMessage> tuple = reqMessageConcurrentHashMap.get(messageId);
        if (tuple == null) {
            return null;
        }
       return tuple.getT2();
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

    /**
     * 返回响应消息
     * @param resp
     */
    @Override
    public void response(RespMessage resp) {
        // TODO 有问题，resp的messageId要和req的一样
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
