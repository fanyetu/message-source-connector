package com.capinfo.kafkademo.common.message.helper;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.*;

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

    public static ReqMessage jsonToMessage(String json) {
        ReqMessage req = new ReqMessage();
        JSONObject jsonObject = JSONUtil.parseObj(json);
        req.setContent(jsonObject.getStr("content"));
        req.setMessageId(jsonObject.getStr("message_id"));
        req.setSourceTopic(jsonObject.getStr("source_topic"));
        JSONObject targetTopic = jsonObject.getJSONObject("target_topic");
        if (targetTopic != null) {
            req.setTargetTopic(targetTopic.getStr("string"));
        }

        return req;
    }

}
