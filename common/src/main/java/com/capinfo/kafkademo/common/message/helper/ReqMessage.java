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
public class ReqMessage extends BaseMessage {

    @Builder(builderMethodName = "of")
    public ReqMessage(String sourceTopic, String content, String targetService) {
        super(sourceTopic, content);
        this.targetService = targetService;
    }

    private String targetService;

}
