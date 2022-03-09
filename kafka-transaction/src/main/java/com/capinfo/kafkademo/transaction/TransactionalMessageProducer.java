package com.capinfo.kafkademo.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * @author zhanghaonan
 * @date 2022/2/28
 */
public class TransactionalMessageProducer {

    private static final String DATA_MESSAGE_1 = "Put any space separated data here for count";
    private static final String DATA_MESSAGE_2 = "Output will contain count of every word in the message";

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = createKafkaProducer();
        // 初始化事务事务
        producer.initTransactions();

        try {
            // 开始事务
            producer.beginTransaction();

            // 发送消息
            Stream.of(DATA_MESSAGE_1, DATA_MESSAGE_2)
                    .forEach(s -> producer.send(new ProducerRecord<>("input", null, s)));

            // 提交事务
            producer.commitTransaction();
        } catch (KafkaException e) {
            producer.abortTransaction();
        }
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(TRANSACTIONAL_ID_CONFIG, "prod-0");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(props);
    }
}
