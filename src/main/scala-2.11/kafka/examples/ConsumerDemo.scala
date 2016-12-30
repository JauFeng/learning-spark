package kafka.examples

object ConsumerDemo extends App {

  val consumerApp = new Consumer(KafkaProperties.TOPIC_B, "group_a")
  val consumerDashboard = new Consumer(KafkaProperties.TOPIC_B, "group_b")
  consumerApp.start()
  consumerDashboard.start()
}
