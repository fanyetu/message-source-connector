package com.capinfo.kafkademo.common.message.helper;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
public interface MessageHelper {

    /**
     * 发送消息
     * @param req
     * @return
     */
    boolean send(ReqMessage req);

    /**
     * 发布事件
     * @param event
     * @return
     */
    boolean publish(EventMessage event);

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
    void receive(MessageReceiveHandler handler);

    /**
     * 消费事件
     * @param handler
     */
    void consume(MessageConsumeHandler handler);
}
