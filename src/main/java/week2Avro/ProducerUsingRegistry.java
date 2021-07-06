package week2Avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class ProducerUsingRegistry {

    public static void main(String[] args) throws IOException {
        ProducerUsingRegistry genericRecordProducer = new ProducerUsingRegistry();
        genericRecordProducer.writeMessage();
    }

    public void writeMessage() throws IOException {
        //create kafka producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.140.0.5:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://10.140.0.3:8081");

        Producer<String, GenericRecord> producer = new KafkaProducer<>(properties);

        //avro schema
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(new File("/home/ngocpham/Documents/Text Document/MasterDev/ngocpv22Project/src/main/java/week2Avro/userInfo.json"));

        //prepare the avro record
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("username", "pvnn");
        avroRecord.put("age", 23);
        avroRecord.put("phone", "0123456");

        GenericData.Record addr = new GenericData.Record(schema.getField("address").schema());
        addr.put("street", "Tran Huu Duc");
        addr.put("city", "Ha Noi");
        addr.put("country", "Viet Nam");
        avroRecord.put("address", addr);

        System.out.println(avroRecord);

        //prepare the kafka record
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("ngocpv22.testavro", null, avroRecord);

        producer.send(record);
        //ensures record is sent before closing the producer
        producer.flush();

        producer.close();
    }
}
