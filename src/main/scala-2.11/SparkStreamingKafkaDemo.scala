import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import java.util.Properties
import java.util.concurrent.ExecutionException

import kafka.examples.KafkaProperties


object SparkStreamingKafkaDemo extends App {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("my-sparkstream")


  val sparkStream: StreamingContext = new StreamingContext(sparkConf, Seconds(10))

  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" → "localhost:9092",
    "key.deserializer" → classOf[LongDeserializer],
    "value.deserializer" → classOf[StringDeserializer],
    "group.id" → "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" → "latest",
    "enable.auto.commit" → (false: java.lang.Boolean)
  )

  val topics: Set[String] = Set(KafkaProperties.TOPIC_A, KafkaProperties.TOPIC_B, KafkaProperties.TOPIC_C)

  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    sparkStream,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )


  //  stream.map(record ⇒ (record.key, record.value)).print()
  stream.count().foreachRDD {
    rdd ⇒
      rdd.foreach(Producer.sendMessage(KafkaProperties.TOPIC_B, _))
  }

  sparkStream.start()
  sparkStream.awaitTermination()
}


object Producer {

  val props: Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer")

  val producer = new KafkaProducer[Long, Long](props)

  def sendMessage(topic: String, message: Long): Unit = {
    try {
      producer.send(new ProducerRecord[Long, Long](topic, System.currentTimeMillis(), message))
    } catch {
      case e: InterruptedException  ⇒
        e.printStackTrace()
    }
  }
}