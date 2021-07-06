package week1Kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerHandleTextFile {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.140.0.5:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "201");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        // đọc các message của topic từ thời điểm hiển tại (default)
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // đọc tất cả các message  của topic từ offset ban đầu
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("ngocpv22.testfile"));
        String data = "";
        Map<String, Integer> datas = new HashMap<>();
        datas.put("windmill", 0);
        datas.put("don", 0);
        datas.put("quixote", 0);
        datas.put("manuel", 0);
        datas.put("combat", 0);
        datas.put("man", 0);
        datas.put("woman", 0);
        datas.put("lady", 0);
        datas.put("mountain", 0);
        datas.put("sword", 0);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records){
                if(record.value() == null) continue;
                String valueLine = record.value().toLowerCase();
                String[] arrString = valueLine.split("[^a-zA-Z]+");
                for(String i : arrString){
                    datas.forEach((key,value) -> {
                        if(key.equals(i)){
                            datas.computeIfPresent(key, (k, v) -> v + 1);
                        }
                    });
                }
                datas.forEach((k,v) -> System.out.println("Key = "
                        + k + ", Value = " + v));
            }
        }
    }
}