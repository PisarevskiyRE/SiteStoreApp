import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.elasticsearch.sink.{Elasticsearch7SinkBuilder, ElasticsearchSink, RequestIndexer}
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, OutputTag}
import org.apache.http.HttpHost
import scheme.Constants.kafkaTopic
import scheme._
import source.KafSource
import transforms._

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalTime, ZoneId, ZonedDateTime}


object Main extends App {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment
    .getExecutionEnvironment


  val kafkaSource =
    new KafSource[Event, EventDeserializer](
      Constants.kafkaHost,
      new EventDeserializer()
    ).getSource(Constants.kafkaTopic)

  val kafkaStream: DataStream[Event] = env.fromSource(
    kafkaSource,
    WatermarkStrategy.noWatermarks(),
    Constants.kafkaLabel)


  val clearStraem = kafkaStream
    .assignTimestampsAndWatermarks(
      new Watermark(100).getWatermarkStrategy[Event]()
    )
    .filter(new FilterEvents[Event](Constants.events))



  val processStream: SingleOutputStreamOperator[Item] = clearStraem
    .keyBy(new EventKeySelector[Event])
    .window(
      SlidingProcessingTimeWindows.of(
        Time.seconds(Constants.lenWindow),
        Time.seconds(Constants.stepWindows)
      )
    )
    .apply(new EventApply[Event, Item]())
    .returns(Types.GENERIC(classOf[Item]))
    .process(new SplitItemsOutside[Item, OutputItem](
      Constants.topBay,
      Constants.topShow
    ))


  val outTopBay =  processStream.getSideOutput(Constants.topBay)
  val outTopShow = processStream.getSideOutput(Constants.topShow)


  val host = new HttpHost("127.0.0.1", 9200, "http")
  val sinkBay = new SinkElastic(host).getSink("idx_bay")
  val sinkShow = new SinkElastic(host).getSink("idx_show")
  val sinkFull = new SinkElastic(host).getSinkFull("idx_full")

  outTopBay.sinkTo(sinkBay)
  outTopShow.sinkTo(sinkShow)
  processStream.sinkTo(sinkFull)



  env.execute()

}
