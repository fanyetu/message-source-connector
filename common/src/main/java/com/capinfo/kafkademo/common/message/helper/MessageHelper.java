package com.capinfo.kafkademo.common.message.helper;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
public interface MessageHelper {

    /**
     * 发送消息
     *
     * @param req
     */
    String send(ReqMessage req);

    /**
     * 回复消息
     * @param resp
     */
    void response(RespMessage resp);

    /**
     * 发布事件
     *
     * @param event
     */
    String publish(EventMessage event);

    String getInstanceKey();

    /**
     * 同步调用
     *
     * @param req
     * @return
     */
    RespMessage invoke(ReqMessage req);

    /**
     * 接收返回消息
     *
     * @param topic
     * @param handler
     */
    void startReceive(String topic, MessageReceiveHandler handler);


    /**
     * 接收消息并返回
     *
     * @param handler
     */
    void startResponse(String topic, MessageResponseHandler handler);

    /**
     * 消费事件
     *
     * @param handler
     */
    void startConsume(String topic, MessageConsumeHandler handler);
}
