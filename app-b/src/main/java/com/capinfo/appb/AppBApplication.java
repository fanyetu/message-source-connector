package com.capinfo.appb;

import com.capinfo.kafkademo.common.message.helper.KafkaMessageHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan(basePackages = "com.capinfo")
@EnableJpaRepositories(basePackages = "com.capinfo")
@ComponentScan(basePackages = "com.capinfo")
@Slf4j
public class AppBApplication implements InitializingBean {

    public static void main(String[] args) {
        SpringApplication.run(AppBApplication.class, args);
    }

    @Autowired
    private KafkaMessageHelper kafkaMessageHelper;

    @Override
    public void afterPropertiesSet() throws Exception {
        kafkaMessageHelper.startResponse("app-b-message", (req, resp) -> {
            resp.setContent("send from app-b");
            return resp;
        });

        kafkaMessageHelper.startConsume("app-a-event", event -> {
            log.info("===================处理事件消息==================\r\n{}\r\n=================================", event);
        });
    }
}
