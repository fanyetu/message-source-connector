package com.capinfo.kafkademo.common.message.helper;

import lombok.*;

/**
 * @author zhanghaonan
 * @date 2022/2/5
 */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Data
@NoArgsConstructor
public class EventMessage extends BaseMessage {

    @Builder(builderMethodName = "of")
    public EventMessage(String messageId, String sourceTopic, String content) {
        super(messageId, sourceTopic, content);
    }
}
