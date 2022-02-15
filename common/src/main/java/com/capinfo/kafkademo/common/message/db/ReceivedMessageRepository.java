package com.capinfo.kafkademo.common.message.db;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author zhanghaonan
 * @date 2022/2/14
 */
public interface ReceivedMessageRepository extends JpaRepository<ReceivedMessage, Long> {

    ReceivedMessage findReceivedMessageByMessageId(String messageId);
}
