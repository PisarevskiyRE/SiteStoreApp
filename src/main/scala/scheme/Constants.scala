package scheme

import org.apache.flink.util.OutputTag

object Constants {
  val defaultFormat = "yyyy-MM-dd HH:mm:ss"
  val errorSerializeLabal = "Ошибка формата"
  val kafkaHost = "localhost:9092"
  val kafkaTopic = "user_behavior"
  val kafkaLabel = "Kafka-Source"
  val events: Seq[String] = Seq("buy","pv")
  val eventBay = "buy"
  val eventShow = "pv"
  val lenWindow: Int = 60
  val stepWindows: Int = 30


  val topBay = new OutputTag[OutputItem]("top-bay-side-output") {}
  val topShow = new OutputTag[OutputItem]("top-show-side-output") {}

}
