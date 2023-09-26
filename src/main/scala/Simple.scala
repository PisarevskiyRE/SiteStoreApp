import io.circe.generic.extras.Configuration
import io.circe.parser._
import io.circe.{Decoder, DecodingFailure, HCursor}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.language.experimental.macros
import scala.util.Try


case class Event(
                  user_id: Int,
                  item_id: Int,
                  category_id: Int,
                  behavior: String,
                  ts: LocalDateTime)
object Event {
  implicit val customConfig: Configuration = Configuration.default.withDefaults

  implicit val decoder: Decoder[Event] = new Decoder[Event] {
    final def apply(c: HCursor): Decoder.Result[Event] =

      for {
        user_id <- c.downField("user_id").as[Int]
        item_id <- c.downField("item_id").as[Int]
        category_id <- c.downField("category_id").as[Int]
        behavior <- c.downField("behavior").as[String]
        tsStr <- c.downField("ts").as[String]
        ts <- Try(LocalDateTime.parse(tsStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
          .toEither
          .left.map(e => DecodingFailure(e.getMessage, c.history))

      } yield Event(user_id, item_id, category_id, behavior, ts)
  }
}







object Simple extends App{



  val env: StreamExecutionEnvironment = StreamExecutionEnvironment
    .getExecutionEnvironment

  val kafkaSource: KafkaSource[Event] = KafkaSource.builder[Event]()
    .setBootstrapServers("localhost:9092")
    .setTopics("user_behavior")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new EventDeserializer())
    .build()


  val kafkaStream: DataStream[Event] = env.fromSource(
    kafkaSource,
    WatermarkStrategy.noWatermarks(),
    "Kafka-Source") //source name


  kafkaStream.print()

  env.execute()
}


class EventDeserializer extends DeserializationSchema[Event] {

  import Event.decoder

  override def deserialize(message: Array[Byte]): Event = {

    val jsonString = new String(message, StandardCharsets.UTF_8)
    decode[Event](jsonString) match {
      case Right(event) => event
      case Left(error) => throw new RuntimeException(s"Failed to deserialize JSON: $error")
    }

  }

  override def isEndOfStream(nextElement: Event): Boolean = false
  implicit val typeInfo = TypeInformation.of(classOf[Event])
  override def getProducedType: TypeInformation[Event] = implicitly[TypeInformation[Event]]
}