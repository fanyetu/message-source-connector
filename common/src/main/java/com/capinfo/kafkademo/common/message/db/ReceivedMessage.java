package com.capinfo.kafkademo.common.message.db;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

/**
 * @author zhanghaonan
 * @date 2022/2/14
 */
@Entity
@Table(name = "t_received_message")
@Data
public class ReceivedMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message_id", nullable = false)
    private String messageId;

    @Column(name = "topic")
    private String topic;

    @Column(name = "content", nullable = false, length = 2000)
    private String content;

    @Column(name = "receive_time")
    private Date receiveTime;

    @Column(name = "instance_key")
    private String instanceKey;
}
