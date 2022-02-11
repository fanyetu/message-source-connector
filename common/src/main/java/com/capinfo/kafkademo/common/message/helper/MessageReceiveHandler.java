package com.capinfo.kafkademo.common.message.helper;

@FunctionalInterface
public interface MessageReceiveHandler {

    void receive(ReqMessage req, RespMessage resp);

}
