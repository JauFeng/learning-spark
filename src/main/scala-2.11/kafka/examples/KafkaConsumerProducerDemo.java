package kafka.examples;

public class KafkaConsumerProducerDemo {

    public static void main(String[] args) {
        boolean isAsync = true;

        Producer producerThread = new Producer(KafkaProperties.TOPIC_A, isAsync);
        producerThread.start();

        Consumer consumerThread = new Consumer(KafkaProperties.TOPIC_B, "groupId");
        consumerThread.start();
    }
}
