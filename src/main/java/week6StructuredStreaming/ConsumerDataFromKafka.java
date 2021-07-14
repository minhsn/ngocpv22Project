package week6StructuredStreaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class ConsumerDataFromKafka {
    public static void main(String[] str) throws InterruptedException, IOException, TimeoutException {

        System.out.println("Starting AutoOffsetAvroConsumerExample ...");

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.140.0.5:9092");
        String consumeGroup = java.util.UUID.randomUUID().toString();
        props.put("group.id", consumeGroup);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "100");
        props.put("heartbeat.interval.ms", "3000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        String topic = "data_tracking_ngocpv22";

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);

        // Assign to specific topic and partition, subscribe could be used here to subscribe to all topic.
        consumer.assign(Arrays.asList(new TopicPartition(topic, 0)));

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, byte[]> record : records) {
                Message.DataTracking dttk = Message.DataTracking.parseFrom(record.value());
                System.out.printf("\n\roffset = %d, key = %s, value = %s", record.offset(), record.key(), dttk);
            }

            //System.out.println("lastOffset read: " + lastOffset);
            consumer.commitSync();
            Thread.sleep(500);
        }
    }
}
