package com.capinfo.appa;

import com.capinfo.kafkademo.common.message.db.Message;
import com.capinfo.kafkademo.common.message.db.MessageRepository;
import com.capinfo.kafkademo.common.message.helper.*;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = AppAApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AppAApplicationTests {

    private final Logger logger = LoggerFactory.getLogger(AppAApplicationTests.class);

    @Resource
    private KafkaMessageHelper kafkaMessageHelper;

    @Resource
    private MessageRepository messageRepository;

    @Test
    @SneakyThrows
    public void testStartReceive() {
        CountDownLatch cdl = new CountDownLatch(1);
        kafkaMessageHelper.startReceive("app-a", ((req, resp) -> {
            logger.info("get resp message, {}", resp.toString());
        }));

        cdl.await();
    }

    @Test
    @SneakyThrows
    public void testStartResponse() {
        CountDownLatch cdl = new CountDownLatch(1);
        kafkaMessageHelper.startResponse("app-b", (req, resp) -> {
            logger.info("get req message, {}", req.toString());
            resp.setContent("test response message");
            cdl.countDown();
            return resp;
        });

        cdl.await();
        Thread.sleep(1000);
    }

    @Test
    public void testKafkaMessageHelperPublish() {
        kafkaMessageHelper.publish(EventMessage.of()
                .sourceTopic("app-a-event")
                .content("hello event")
                .build());
    }

    @Test
    public void testKafkaMessageHelperSend() {
        kafkaMessageHelper.send(ReqMessage.of()
                .sourceTopic("app-a")
                .targetTopic("app-b")
                .content("hello world")
                .build());
    }

    @Test
    public void testMessageRepository() {
        List<Message> all = messageRepository.findAll();
        System.out.println(all);
    }

}
