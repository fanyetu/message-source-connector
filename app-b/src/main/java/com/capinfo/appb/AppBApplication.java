package com.capinfo.appb;

import cn.hutool.core.util.IdUtil;
import com.capinfo.appb.db.Test;
import com.capinfo.appb.db.TestRepository;
import com.capinfo.kafkademo.common.message.helper.KafkaMessageHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@SpringBootApplication
@EntityScan(basePackages = "com.capinfo")
@EnableJpaRepositories(basePackages = "com.capinfo")
@ComponentScan(basePackages = "com.capinfo")
@Slf4j
@RestController
@RequestMapping("/app-b")
public class AppBApplication implements InitializingBean {

    @Resource
    private TestRepository testRepository;

    public static void main(String[] args) {
        SpringApplication.run(AppBApplication.class, args);
    }

    @PostMapping("/test")
    public String test(@RequestBody String data) {
        Test test = new Test();
        test.setContent(IdUtil.fastSimpleUUID());
        testRepository.save(test);
        return data.toUpperCase();
    }

    @Autowired
    private KafkaMessageHelper kafkaMessageHelper;

    @Override
    public void afterPropertiesSet() throws Exception {
        kafkaMessageHelper.startResponse("app-b-test", (req, resp) -> {
            resp.setContent(req.getContent().toUpperCase());

            Test test = new Test();
            test.setContent(IdUtil.fastSimpleUUID());
            testRepository.save(test);
            return resp;
        });

        kafkaMessageHelper.startResponse("app-b-message", (req, resp) -> {
            resp.setContent("send from app-b");
            return resp;
        });

        kafkaMessageHelper.startConsume("app-a-event", event -> {
            log.info("===================处理事件消息==================\r\n{}\r\n=================================", event);
        });
    }
}
