package com.capinfo.kafkademo.common.message.db;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
@Entity
@Table(name = "t_message")
@Data
public class Message {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message_id", nullable = false)
    private String messageId;

    @Column(name = "target_topic")
    private String targetTopic;

    @Column(name = "source_topic", nullable = false)
    private String sourceTopic;

    @Column(name = "content", nullable = false, length = 2000)
    private String content;

    @Column(name = "create_time")
    private Date createTime;

    @Column(name = "instance_key")
    private String instanceKey;

    @Column(name = "process_flag")
    private String processFlag;

    @Column(name = "message_type")
    private String messageType;
}
