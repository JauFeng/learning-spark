import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import org.apache.kafka.common.serialization.{
  LongDeserializer,
  StringDeserializer
}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import java.util.Properties
import java.util.concurrent.ExecutionException

import kafka.examples.KafkaProperties
import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator}

import scala.collection.mutable

object SparkStreamingKafkaDemo extends App {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("my-sparkstream")

  val sparkStream: StreamingContext =
    new StreamingContext(sparkConf, Seconds(10))

  val sparkContext = sparkStream.sparkContext

//  val accumulatorSet: CollectionAccumulator[String] =
//    sparkContext.collectionAccumulator[String]("accumulator-set")

  val devices = new SetAccumulatorV2[String]
  sparkContext.register(devices, "devices-set")

  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" → "localhost:9092",
    "key.deserializer" → classOf[LongDeserializer],
    "value.deserializer" → classOf[StringDeserializer],
    "group.id" → "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" → "latest",
    "enable.auto.commit" → (false: java.lang.Boolean)
  )

  val topics: Set[String] = Set(KafkaProperties.TOPIC_A)

  val stream: InputDStream[ConsumerRecord[Long, String]] =
    KafkaUtils.createDirectStream[Long, String](
      sparkStream,
      PreferConsistent,
      Subscribe[Long, String](topics, kafkaParams)
    )

  stream
    .map { record ⇒
      devices.add(record.value)
      (record.key, record.value)
    }
    .count()
    .foreachRDD { l ⇒
      l.foreach { currentNum ⇒
        Producer.sendMessage(KafkaProperties.TOPIC_B,
                             currentNum,
                             devices.value.size)
      }
    }

  sparkStream.start()
  sparkStream.awaitTermination()
}

object Producer {

  val props: Properties = new Properties()
  props.put(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
    KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.LongSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[Long, String](props)

  def sendMessage(topic: String, currentNum: Long, totalNum: Long): Unit = {
    val message = currentNum + ", " + totalNum

    producer.send(
      new ProducerRecord[Long, String](topic,
                                       System.currentTimeMillis(),
                                       message))
  }
}

class SetAccumulatorV2[T] extends AccumulatorV2[T, mutable.Set[T]] {

  private val _set: mutable.Set[T] = mutable.Set[T]()

  override def isZero: Boolean = _set.isEmpty

  override def copy(): SetAccumulatorV2[T] = {
    val newAcc = new SetAccumulatorV2[T]
    _set.synchronized(
      newAcc._set ++= _set
    )
    newAcc
  }

  override def reset(): Unit = _set.clear()

  override def add(v: T): Unit = _set.add(v)

  override def merge(other: AccumulatorV2[T, mutable.Set[T]]): Unit =
    other match {
      case s: SetAccumulatorV2[T] ⇒ _set.++=(s.value)
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }

  override def value: mutable.Set[T] = _set
}
