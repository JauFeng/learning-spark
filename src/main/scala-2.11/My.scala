import java.sql.Date

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.Map

object My extends App {

  val sparkConf = new SparkConf()
    .setAppName("my")
    .setMaster("local[*]")

  val sparkContext = SparkContext.getOrCreate(sparkConf)

  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


  import sparkSession.implicits._


//  val df: DataFrame = sparkSession.read.json("/home/freecloud/IdeaProjects/learning-spark/src/main/resources/IncreaseId.json")
  val df: DataFrame = sparkSession.read.json("hdfs://localhost:9000/user/flume/events/user-info/*.log")

  val result: RDD[(Date, String)] = df.rdd
    .map(row ⇒ (row.getAs[String]("device_id"), row.getAs[String]("client_time").toLong))
    .reduceByKey(math.min)
    .map(t ⇒ (new Date(t._2), t._1))

  val increaseUser = result.toDF().select($"_1" as "date", $"_2" as "device_id")


  val user = df.select($"terminal_os", $"device_id").distinct()

  increaseUser.show()


  user.show()

//  increaseUser.createOrReplaceTempView("increaseUser")

  sparkSession.stop()

}
