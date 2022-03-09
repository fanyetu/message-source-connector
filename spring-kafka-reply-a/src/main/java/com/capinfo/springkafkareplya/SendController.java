package com.capinfo.springkafkareplya;

import cn.hutool.core.util.IdUtil;
import com.capinfo.springkafkareplya.db.Test;
import com.capinfo.springkafkareplya.db.TestRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author zhanghaonan
 * @date 2022/3/1
 */
@RestController
@RequestMapping("/app-a")
public class SendController {

    @Resource
    private ReplyingKafkaTemplate<String, String, String> replyingTemplate;

    @Resource
    private TestRepository testRepository;

    @PostMapping("/test")
    public String send(@RequestBody String data) throws InterruptedException, ExecutionException, TimeoutException {
        ProducerRecord<String, String> record = new ProducerRecord<>("kRequests", data);
        RequestReplyFuture<String, String, String> replyFuture = replyingTemplate.sendAndReceive(record);

        SendResult<String, String> sendResult = replyFuture.getSendFuture().get(60, TimeUnit.SECONDS);
        System.out.println("Sent ok: " + sendResult.getRecordMetadata());

        ConsumerRecord<String, String> consumerRecord = replyFuture.get(60, TimeUnit.SECONDS);
        String result = consumerRecord.value();
        System.out.println("Return value: " + result);

        Test test = new Test();
        test.setContent(IdUtil.fastSimpleUUID());
        testRepository.save(test);

        return result;
    }
}
