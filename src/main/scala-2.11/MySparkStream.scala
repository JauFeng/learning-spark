import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MySparkStream extends App {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("my-sparkstream")


  val sparkStream = new StreamingContext(sparkConf, Seconds(1))

  val text: DStream[String] = sparkStream.textFileStream("/home/freecloud/IdeaProjects/learning-spark/src/main/resources/people.json")

  text.count().print()


  sparkStream.start()
  sparkStream.awaitTermination()

}
