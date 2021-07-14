package week6StructuredStreaming;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerPushDataToKafka {
    public static void main(String[] args) throws InterruptedException, IOException, ExecutionException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.140.0.5:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Producer<String, byte[]> producer = new KafkaProducer(props);

        System.out.println("Starting Producer push random data to kafka ...");

        String topic = "data_tracking_ngocpv22_test";
        int partition = 0;
        Faker faker = new Faker();

        for(int i = 0; i < 10000; i ++){

            Message.DataTracking dataTrackingBuilder = Message.DataTracking.newBuilder()
                    .setVersion("version-" + faker.number().numberBetween(1, 9) + "." + faker.number().numberBetween(0, 20))
                    .setName(faker.name().fullName())
                    .setTimestamp(faker.number().numberBetween(1286705410, 1633860610))
                    .setPhoneId(faker.phoneNumber().phoneNumber())
                    .setLon(faker.number().numberBetween(0, 999))
                    .setLat(faker.number().numberBetween(0, 999))
                    .build();

            byte[] record = dataTrackingBuilder.toByteArray();

            producer.send(new ProducerRecord<String, byte[]>(topic, partition, Integer.toString(i), record));

            Thread.sleep(500);
        }
    }
}
