package com.capinfo.kafkademo.common.message.db;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
public interface MessageRepository extends JpaRepository<Message, Long> {


}
