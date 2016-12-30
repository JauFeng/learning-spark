package kafka.examples;

public class KafkaProperties {

    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    public static final int CONNECTION_TIMEOUT = 100000;
    public static final String TOPIC_A = "topic-A";
    public static final String TOPIC_B = "topic-B";
    public static final String TOPIC_C = "topic-C";
    public static final String CLIENT_ID = "MyConsumerClient";


    private KafkaProperties() {
    }
}
