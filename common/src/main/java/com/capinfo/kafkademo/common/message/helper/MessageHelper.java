package com.capinfo.kafkademo.common.message.helper;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
public interface MessageHelper {

    /**
     * 发送消息
     * @param req
     */
    void send(ReqMessage req);

    /**
     * 发布事件
     * @param event
     */
    void publish(EventMessage event);

    /**
     * 同步调用
     * @param req
     * @return
     */
    RespMessage invoke(ReqMessage req);

    /**
     * 接收消息
     * @param handler
     */
    void startReceive(String topic, MessageReceiveHandler handler);

    /**
     * 消费事件
     * @param handler
     */
    void startConsume(String topic, MessageConsumeHandler handler);
}
