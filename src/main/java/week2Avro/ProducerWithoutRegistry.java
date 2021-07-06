package week2Avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * Create and publish an avro type message.
 */
public class ProducerWithoutRegistry {

    public static void main(String[] str) throws InterruptedException, IOException {

        System.out.println("Starting Producer AVRO without registry ...");

        Producer<String, byte[]> producer = createProducer();

        sendRecords(producer);
    }

    private static Producer<String, byte[]> createProducer() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.140.0.5:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        return new KafkaProducer(props);
    }

    private static void sendRecords(Producer<String, byte[]> producer) throws IOException, InterruptedException {
        String topic = "ngocpv22.testavro";
        int partition = 0;

        producer.send(new ProducerRecord<String, byte[]>(topic, partition, Integer.toString(0), record(10 + "")));

        Thread.sleep(500);
    }


    private static byte[] record(String name) throws IOException {

        GenericRecord record = new GenericData.Record(AvroSupport.getSchema());

        //prepare the avro record
        record.put("username", "ngocpv223");
        record.put("age", 23);
        record.put("phone", "0123456");

        GenericData.Record addr = new GenericData.Record(AvroSupport.getSchema().getField("address").schema());
        addr.put("street", "Tran Huu Duc");
        addr.put("city", "Ha Noi");
        addr.put("country", "Viet Nam");
        record.put("address", addr);

        return AvroSupport.dataToByteArray(AvroSupport.getSchema(), record);

    }


}