package com.capinfo.kafkademo.common.message.helper;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhanghaonan
 * @date 2022/2/10
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class BaseMessage {

    private String messageId;

    private String sourceTopic;

    private String content;
}
