package com.capinfo.kafkademo.common.message.helper;

import lombok.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

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

    public static EventMessage avroToMessage(GenericData.Record value) {
        EventMessage event = new EventMessage();
        event.setMessageId(((Utf8) value.get("message_id")).toString());
        event.setContent(((Utf8) value.get("content")).toString());
        event.setSourceTopic(((Utf8) value.get("source_topic")).toString());
        return event;
    }
}
