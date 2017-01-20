import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Date

object MyTest extends App {

  val sparkConf = new SparkConf()
    .setAppName("text")
    .setMaster("local")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sparkContext = new SparkContext(sparkConf)

  val sparkSession = SparkSession
    .builder()
    .appName("test")
    .config(sparkConf)
    .getOrCreate()

  import sparkSession.implicits._

  val dataFrame = sparkSession.read.json(
    "hdfs://iz2ze7ykmwwdhm8z48nx6pz:8020/user/flume/events/user-info/*.log")

  val incUserRdd: RDD[(Date, String)] = dataFrame.rdd
    .map(row ⇒
      (row.getAs[String]("device_id"),
       row.getAs[String]("client_time").toLong))
    .reduceByKey(math.min)
    .map(t ⇒ (new Date(t._2), t._1))

  val increaseUser: DataFrame =
    incUserRdd.toDF().select($"_1" as "date", $"_2" as "device_id")
  increaseUser.registerTempTable("increaseUser")

  val user: DataFrame =
    dataFrame.select($"terminal_os", $"device_id").distinct()
  user.registerTempTable("user")

}
