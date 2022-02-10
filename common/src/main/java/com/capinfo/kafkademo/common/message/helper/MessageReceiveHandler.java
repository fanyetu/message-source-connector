package com.capinfo.kafkademo.common.message.helper;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
@FunctionalInterface
public interface MessageReceiveHandler {

    RespMessage receive(ReqMessage req);

}
