package kafka.examples

object ProducerDemo extends App {

  val producer = new Producer(KafkaProperties.TOPIC_A, false)
  producer.start()

}
