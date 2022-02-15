package com.capinfo.kafkademo.common.message.db;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
public interface MessageRepository extends JpaRepository<Message, Long> {

    @Modifying
    @Transactional
    @Query("update Message set processFlag = '1' where messageId = :messageId")
    void updateProcessFlag(@Param(value = "messageId") String messageId);

    Message findMessageByMessageId(String messageId);


}
