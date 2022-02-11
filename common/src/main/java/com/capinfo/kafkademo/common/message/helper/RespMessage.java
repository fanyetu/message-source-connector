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
public class RespMessage extends BaseMessage {

    @Builder(builderMethodName = "of")
    public RespMessage(String messageId, String sourceTopic, String content, String targetService) {
        super(messageId, sourceTopic, content);
        this.targetTopic = targetService;
    }

    private String targetTopic;

    public static RespMessage avroToMessage(GenericData.Record value) {
        RespMessage resp = new RespMessage();
        resp.setMessageId(((Utf8) value.get("message_id")).toString());
        resp.setContent(((Utf8) value.get("content")).toString());
        resp.setSourceTopic(((Utf8) value.get("source_topic")).toString());
        resp.setTargetTopic(((Utf8) value.get("target_topic")).toString());
        return resp;
    }
}
