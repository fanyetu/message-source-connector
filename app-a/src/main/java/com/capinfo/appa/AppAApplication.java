package com.capinfo.appa;

import com.capinfo.kafkademo.common.message.helper.EventMessage;
import com.capinfo.kafkademo.common.message.helper.KafkaMessageHelper;
import com.capinfo.kafkademo.common.message.helper.ReqMessage;
import com.capinfo.kafkademo.common.message.helper.RespMessage;
import com.sun.org.apache.xml.internal.security.Init;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@SpringBootApplication
@EntityScan(basePackages = "com.capinfo")
@EnableJpaRepositories(basePackages = "com.capinfo")
@ComponentScan(basePackages = "com.capinfo")
@EnableKafka
@RestController
@RequestMapping("/app-a")
@Slf4j
public class AppAApplication implements InitializingBean {

    @Autowired
    private KafkaMessageHelper kafkaMessageHelper;

    public static void main(String[] args) {
        SpringApplication.run(AppAApplication.class, args);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        kafkaMessageHelper.startReceive("app-a-message-async", (req, resp) -> {
            log.info("===================处理返回消息==================\r\n{}\r\n=================================", resp);
        });
    }

    @PostMapping("/send")
    public String send(@RequestBody Map<String, String> data) {
        ReqMessage reqMessage = new ReqMessage();
        reqMessage.setSourceTopic("app-a-message-async");
        reqMessage.setTargetTopic("app-b-message");
        reqMessage.setContent(data.get("content"));
        kafkaMessageHelper.send(reqMessage);
        return "success";
    }

    @PostMapping("/publish")
    public String publish(@RequestBody Map<String, String> data) {
        EventMessage event = new EventMessage();
        event.setSourceTopic("app-a-event");
        event.setContent(data.get("content"));
        kafkaMessageHelper.publish(event);
        return "success";
    }

    @PostMapping("/invoke")
    public RespMessage invoke(@RequestBody Map<String, String> data) {
        ReqMessage reqMessage = new ReqMessage();
        reqMessage.setSourceTopic("app-a-message");
        reqMessage.setTargetTopic("app-b-message");
        reqMessage.setContent(data.get("content"));
        return kafkaMessageHelper.invoke(reqMessage);
    }
}
