import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import scheme._
import source.KafSource
import transforms._


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


  val host = Constants.elasticHost
  val sinkBay = new SinkElastic(host).getSink("idx_bay")
  val sinkShow = new SinkElastic(host).getSink("idx_show")
  val sinkFull = new SinkElastic(host).getSinkFull("idx_full")


  outTopBay.sinkTo(sinkBay)
  outTopShow.sinkTo(sinkShow)
  processStream.sinkTo(sinkFull)

  env.execute()
}
