package com.capinfo.kafkademo.transaction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * @author zhanghaonan
 * @date 2022/2/28
 */
public class TransactionalWordCount {

    private static final String CONSUMER_GROUP_ID = "my-group-id";
    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        KafkaProducer<String, String> producer = createKafkaProducer();

        producer.initTransactions();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(60));

                Map<String, Integer> wordCountMap = records.records(new TopicPartition(INPUT_TOPIC, 0))
                        .stream()
                        .flatMap(record -> Stream.of(record.value()
                                .split(" ")))
                        .map(word -> Tuple.of(word, 1))
                        .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue, Integer::sum));

                producer.beginTransaction();
                wordCountMap.forEach((key, value) -> producer.send(new ProducerRecord<String, String>(OUTPUT_TOPIC, key, value.toString())));

                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
                    long offset = partitionedRecords.get(partitionedRecords.size() - 1)
                            .offset();

                    offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
                }

                /*
                将指定的偏移量列表发送给消费者组协调人，同时将这些偏移量标记为当前交易的一部分。
                只有当事务成功提交时，这些偏移量才会被视为已提交。提交的偏移量应该是你的应用程序将消费的下一条消息，
                即lastProcessedMessageOffset + 1。
                需要把消费的和生产的消息批在一起时，应该使用这个方法，通常是在消费-转换-生产模式中。
                因此，指定的consumerGroupId应该与所用消费者的配置参数group.id相同。注意，消费者应该有enable.auto.commit=false，
                也不应该手动提交偏移量（通过同步或异步提交）。
                 */
                producer.sendOffsetsToTransaction(offsetsToCommit, CONSUMER_GROUP_ID);
                producer.commitTransaction();
            }
        } catch (KafkaException e) {
            producer.abortTransaction();
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(singleton(INPUT_TOPIC));
        return consumer;
    }

    private static KafkaProducer<String, String> createKafkaProducer() {

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(TRANSACTIONAL_ID_CONFIG, "prod-1");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(props);

    }
}
