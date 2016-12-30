import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object My2 extends App {

  val sparkConf = new SparkConf()
    .setAppName("my")
    .setMaster("local[*]")

  val sparkContext = SparkContext.getOrCreate(sparkConf)

  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


  import sparkSession.implicits._


  val s = """{"device_id" : "Iphone6s01", "is_sign_in" : "true","sign_in_type" : "3rd-part", "user_id" : "100000", "open_user_id" : "wechat_balbalbal", "open_user_type" : "wechat", "client_time" : "2016-11-17 19:23", "terminal_os" : "IOS", "ver" : "2.5.6", "channel" : "AppleStore", "ip" : "10.202.130.128", "network" : "WIFI", "launch_time" : "4", "online_time" : "3600000", "online_duration" : "millisecond", "action_list" : [ { "action_id" : "view_video_detail", "from_page_id" : "discovery", "page_area_id" : "banner", "to_page_id" : "video_detail" }, { "action_id" : "star_video", "from_page_id" : "video_detail", "page_area_id" : "video_star", "to_page_id" : "video_detail" } ], "play_list": [ { "video_id" : "32207", "video_name" : "色狼别闹01", "time" : "300000", "duration" : "millisecond" }, { "video_id" : "12001", "video_name" : "色狼别闹02", "time" : "400000", "duration" : "millisecond" } ]}"""


//  val df: DataFrame = sparkSession.read.json("/home/freecloud/IdeaProjects/learning-spark/src/main/resources/info.json")
  val df: DataFrame = sparkSession.read.json(s)

  df.select($"device_id", $"client_time").show()




  sparkContext.stop()
  sparkSession.stop()
}
