package com.capinfo.springkafkatransactiona.service;

import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.RandomUtil;
import com.capinfo.springkafkatransactiona.db.Test;
import com.capinfo.springkafkatransactiona.db.TestRepository;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * @author zhanghaonan
 * @date 2022/3/8
 */
@Service
public class SendService {

    @Resource
    private ReplyingKafkaTemplate<String, String, String> replyingTemplate;

    @Resource
    private TestRepository testRepository;

    @Transactional
    @SneakyThrows
    public String sendWithTransactionAsync(String data) {
        ProducerRecord<String, String> record = new ProducerRecord<>("kEvents", data);
        ListenableFuture<SendResult<String, String>> sendFuture = replyingTemplate.send(record);
        SendResult<String, String> sendResult = sendFuture.get(10, TimeUnit.SECONDS);
        System.out.println("Sent ok: " + sendResult.getRecordMetadata());

        Test test = new Test();
        test.setContent(IdUtil.fastSimpleUUID());
        testRepository.save(test);
//        if (true) {
//            throw new RuntimeException("test");
//        }
        return "success";
    }


    /**
     * 问题：transaction + reply行不通
     * reply回来之前transaction没有提交，但是没有提交transaction，消费者就无法接收到数据（read_committed模式必须producer提交后才能消费）
     * 而reply无法接收数据就无法返回，则producer就无法提交事务，死循环
     * @param data
     * @return
     */
    @Deprecated
    @SneakyThrows
    @Transactional(rollbackFor = Exception.class)
    public String sendWithTransaction(String data) {
        ProducerRecord<String, String> record = new ProducerRecord<>("kRequests", data);
        RequestReplyFuture<String, String, String> replyFuture = replyingTemplate.sendAndReceive(record);

        SendResult<String, String> sendResult = replyFuture.getSendFuture().get(10, TimeUnit.SECONDS);
        System.out.println("Sent ok: " + sendResult.getRecordMetadata());

        ConsumerRecord<String, String> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);
        String result = consumerRecord.value();
        System.out.println("Return value: " + result);

        Test test = new Test();
        test.setContent(IdUtil.fastSimpleUUID());
        testRepository.save(test);

        return result;
    }

    @SneakyThrows
    @Transactional
    public String send(String data) {
        ProducerRecord<String, String> record = new ProducerRecord<>("kRequests", data);
        RequestReplyFuture<String, String, String> replyFuture = replyingTemplate.sendAndReceive(record);
        SendResult<String, String> sendResult = replyFuture.getSendFuture().get(10, TimeUnit.SECONDS);
        System.out.println("Sent ok: " + sendResult.getRecordMetadata());
        ConsumerRecord<String, String> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);
        String result = consumerRecord.value();
        System.out.println("Return value: " + result);
        return result;
    }
}
