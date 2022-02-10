package com.capinfo.appa;

import com.capinfo.kafkademo.common.message.db.Message;
import com.capinfo.kafkademo.common.message.db.MessageRepository;
import com.capinfo.kafkademo.common.message.helper.EventMessage;
import com.capinfo.kafkademo.common.message.helper.KafkaMessageHelper;
import com.capinfo.kafkademo.common.message.helper.ReqMessage;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = AppAApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AppAApplicationTests {

    @Resource
    private KafkaMessageHelper kafkaMessageHelper;

    @Resource
    private MessageRepository messageRepository;

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
                .targetService("app-b")
                .content("hello world")
                .build());
    }

    @Test
    public void testMessageRepository() {
        List<Message> all = messageRepository.findAll();
        System.out.println(all);
    }

}
