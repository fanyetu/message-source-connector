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
public class ReqMessage extends BaseMessage {

    @Builder(builderMethodName = "of")
    public ReqMessage(String messageId, String sourceTopic, String content, String targetTopic) {
        super(messageId, sourceTopic, content);
        this.targetTopic = targetTopic;
    }

    private String targetTopic;

    public static ReqMessage avroToMessage(GenericData.Record value) {
        ReqMessage req = new ReqMessage();
        req.setMessageId(((Utf8) value.get("message_id")).toString());
        req.setContent(((Utf8) value.get("content")).toString());
        req.setSourceTopic(((Utf8) value.get("source_topic")).toString());
        req.setTargetTopic(((Utf8) value.get("target_topic")).toString());
        return req;
    }

}
