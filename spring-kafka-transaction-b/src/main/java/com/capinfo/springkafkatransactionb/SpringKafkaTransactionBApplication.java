package com.capinfo.springkafkatransactionb;

import com.capinfo.springkafkatransactionb.service.ConsumerService;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SimpleKafkaHeaderMapper;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.annotation.Resource;
import javax.persistence.EntityManagerFactory;

/**
 * @author zhanghaonan
 */
@EnableKafka
@SpringBootApplication
@EntityScan(basePackages = "com.capinfo")
@EnableJpaRepositories(basePackages = "com.capinfo")
@EnableTransactionManagement
public class SpringKafkaTransactionBApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaTransactionBApplication.class, args);
    }

    @Resource
    private ConsumerService consumerService;

    @KafkaListener(id="server", topics = "kRequests")
    @SendTo // use default replyTo expression
    public String listen(String in) {

        System.out.println("Server received: " + in);

        return consumerService.consume(in);
    }

    @KafkaListener(id="server_event", topics = "kEvents")
    public void listenEvent(String data) {
        System.out.println("Server received: " + data);
    }

    @Bean
    public NewTopic kEvents() {
        return TopicBuilder.name("kEvents")
                .partitions(10)
                .replicas(2)
                .build();
    }

    @Bean
    public NewTopic kRequests() {
        return TopicBuilder.name("kRequests")
                .partitions(10)
                .replicas(2)
                .build();
    }

    @Bean
    @Primary
    public JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    @Bean
    public KafkaTransactionManager kafkaTransactionManager(final ProducerFactory<String, String> pf) {
        return new KafkaTransactionManager<>(pf);
    }

    @Bean // not required if Jackson is on the classpath
    public MessagingMessageConverter simpleMapperConverter() {
        MessagingMessageConverter messagingMessageConverter = new MessagingMessageConverter();
        messagingMessageConverter.setHeaderMapper(new SimpleKafkaHeaderMapper());
        return messagingMessageConverter;
    }

}
