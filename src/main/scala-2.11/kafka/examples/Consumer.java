package kafka.examples;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class Consumer extends ShutdownableThread {

    private final KafkaConsumer<Long, Long> consumer;
    private final String topic;
    private final String groupId;


    public Consumer(String topic, String groupId) {
        super("KafkaConsumerDemo", false);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.groupId = groupId;
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<Long, Long> records = consumer.poll(1000);
        for (ConsumerRecord<Long, Long> record : records) {
            System.out.println(groupId + " received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        }
    }


    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterrupted() {
        return false;
    }
}
