package com.capinfo.springkafkareplyb;

import cn.hutool.core.util.IdUtil;
import com.capinfo.springkafkareplyb.db.Test;
import com.capinfo.springkafkareplyb.db.TestRepository;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.SimpleKafkaHeaderMapper;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.messaging.handler.annotation.SendTo;

import javax.annotation.Resource;


@EnableKafka
@EntityScan(basePackages = "com.capinfo")
@EnableJpaRepositories(basePackages = "com.capinfo")
@SpringBootApplication
public class SpringKafkaReplyBApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaReplyBApplication.class, args);
    }

    @Resource
    private TestRepository testRepository;

    @KafkaListener(id="server", topics = "kRequests")
    @SendTo // use default replyTo expression
    public String listen(String in) {

        System.out.println("Server received: " + in);
        Test test = new Test();
        test.setContent(IdUtil.fastSimpleUUID());
        testRepository.save(test);

        return in.toUpperCase();
    }

    @Bean
    public NewTopic kRequests() {
        return TopicBuilder.name("kRequests")
                .partitions(10)
                .replicas(2)
                .build();
    }

    @Bean // not required if Jackson is on the classpath
    public MessagingMessageConverter simpleMapperConverter() {
        MessagingMessageConverter messagingMessageConverter = new MessagingMessageConverter();
        messagingMessageConverter.setHeaderMapper(new SimpleKafkaHeaderMapper());
        return messagingMessageConverter;
    }
}
