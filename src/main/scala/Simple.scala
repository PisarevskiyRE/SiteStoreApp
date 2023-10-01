//import io.circe.generic.extras.Configuration
//import io.circe.parser._
//import io.circe.{Decoder, DecodingFailure, HCursor}
//import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
//import org.apache.flink.api.common.functions.FilterFunction
//import org.apache.flink.api.common.serialization.DeserializationSchema
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.connector.sink2.SinkWriter
//import org.apache.flink.api.java.functions.KeySelector
//import org.apache.flink.connector.elasticsearch.sink.{Elasticsearch7SinkBuilder, ElasticsearchSink, RequestIndexer}
//import org.apache.flink.connector.kafka.source.KafkaSource
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.functions.ProcessFunction
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction
//import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.{Collector, OutputTag}
//import org.apache.http.HttpHost
//import org.elasticsearch.action.index.IndexRequest
//import org.elasticsearch.client.Requests
//import org.elasticsearch.common.xcontent.XContentType
//
//import java.lang
//import java.nio.charset.StandardCharsets
//import java.time.format.DateTimeFormatter
//import java.time.{Instant, LocalDateTime, ZoneOffset}
//import scala.jdk.CollectionConverters.{IterableHasAsJava, IterableHasAsScala}
//import scala.util.Try
//
//case class Event(
//                  user_id: Int,
//                  item_id: Int,
//                  category_id: Int,
//                  behavior: String,
//                  ts: Instant)
//
//class EventDeserializer extends DeserializationSchema[Event] {
//
//  import Event.decoder
//
//  override def deserialize(message: Array[Byte]): Event = {
//
//    val jsonString = new String(message, StandardCharsets.UTF_8)
//    decode[Event](jsonString) match {
//      case Right(event) => event
//      case Left(error) => throw new RuntimeException(s"Failed to deserialize JSON: $error")
//    }
//
//  }
//
//  override def isEndOfStream(nextElement: Event): Boolean = false
//
//  implicit val typeInfo = TypeInformation.of(classOf[Event])
//
//  override def getProducedType: TypeInformation[Event] = implicitly[TypeInformation[Event]]
//}
//
////case class Item(itemId: Int, bayCnt: Int, showCnt: Int)
//
//object Event {
//  implicit val customConfig: Configuration = Configuration.default.withDefaults
//
//  implicit val decoder: Decoder[Event] = new Decoder[Event] {
//    final def apply(c: HCursor): Decoder.Result[Event] =
//
//      for {
//        user_id <- c.downField("user_id").as[Int]
//        item_id <- c.downField("item_id").as[Int]
//        category_id <- c.downField("category_id").as[Int]
//        behavior <- c.downField("behavior").as[String]
//        tsStr <- c.downField("ts").as[String]
//        ts <- Try(LocalDateTime.parse(tsStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toInstant(ZoneOffset.UTC))
//          .toEither
//          .left.map(e => DecodingFailure(e.getMessage, c.history))
//
//      } yield Event(user_id, item_id, category_id, behavior, ts)
//  }
//}
//
//object Simple extends App{
//
//
//
//  val env: StreamExecutionEnvironment = StreamExecutionEnvironment
//    .getExecutionEnvironment
//
//  val kafkaSource: KafkaSource[Event] = KafkaSource.builder[Event]()
//    .setBootstrapServers("localhost:9092")
//    .setTopics("user_behavior")
//    .setStartingOffsets(OffsetsInitializer.earliest())
//    .setValueOnlyDeserializer(new EventDeserializer())
//    .build()
//
//
//  val kafkaStream: DataStream[Event] = env.fromSource(
//    kafkaSource,
//    WatermarkStrategy.noWatermarks(),
//    "Kafka-Source") //source name
//
//  val stream = kafkaStream.assignTimestampsAndWatermarks(
//      WatermarkStrategy
//        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(100))
//        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
//          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
//            element.ts.toEpochMilli
//          }
//        })
//    )
//    .filter(new FilterFunction[Event] {
//      override def filter(value: Event): Boolean = value.behavior == "buy" || value.behavior == "pv"
//    })
//    .keyBy(new KeySelector[Event, Int] {
//      override def getKey(value: Event): Int = value.item_id
//    })
//    .window(SlidingProcessingTimeWindows.of(Time.seconds(20), Time.seconds(10)))
//    .apply(new WindowFunction[Event, Item, Int, TimeWindow] {
//      override def apply(
//                          key: Int,
//                          window: TimeWindow,
//                          input: lang.Iterable[Event],
//                          out: Collector[Item]): Unit = {
//
//        val countBuy = input.asScala.count(x => x.behavior == "buy")
//        val countPv = input.asScala.count(x => x.behavior == "pv")
//        out.collect(Item(key, countBuy, countPv))
//      }
//    })
//
//  val topBay = new OutputTag[(Int, Int)]("top-bay-side-output"){}
//  val topShow = new OutputTag[(Int, Int)]("top-show-side-output"){}
//
//  val test = stream
//    .process(new ProcessFunction[Item, Item]{
//      override def processElement(
//                                   value: Item, ctx: ProcessFunction[Item, Item]#Context,
//                                   out: Collector[Item]): Unit = {
//        if (value.bayCnt > 0 && value.showCnt == 0) {
//          ctx.output(topBay,(value.itemId, value.bayCnt))
//        }
//
//        if (value.showCnt > 0 && value.bayCnt == 0 ) {
//          ctx.output(topShow,(value.itemId, value.showCnt))
//        }
//
//      }
//    })
//
//
//
//
//  val outTopBay =  test.getSideOutput(topBay)
//  //val outTopShow =  test.getSideOutput(topShow)
//
//
//
//  def createIndexRequest(element: ((Int, Int))): IndexRequest = {
//
//    val json = Map(
//      element._1 -> element._2,
//    )
//
//    Requests
//      .indexRequest
//      .index("output_test")
//      .source(json.asJava, XContentType.JSON)
//  }
//
//
//  val sink: ElasticsearchSink[(Int, Int)] = new Elasticsearch7SinkBuilder[(Int, Int)]()
//    .setBulkFlushInterval(1000L)
//    .setBulkFlushMaxActions(1)
//    .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
//    .setEmitter((element: (Int, Int), context: SinkWriter.Context, indexer: RequestIndexer) =>
//      indexer.add(
//        createIndexRequest(element)))
//    .build()
//
//  outTopBay.sinkTo(sink)
//
//
//
//  env.execute()
//}
