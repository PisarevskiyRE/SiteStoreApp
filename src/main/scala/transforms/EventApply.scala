package transforms

import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scheme.{Constants, Item, MetricData, RawData}

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsScala

class EventApply[A <: RawData, B <: MetricData] extends  WindowFunction[A, B, Int, TimeWindow] {
  override def apply(
                      key: Int,
                      window: TimeWindow,
                      input: lang.Iterable[A],
                      out: Collector[B]): Unit = {
    val countBuy = input.asScala.count(x => x.behavior == Constants.eventBay)
    val countPv = input.asScala.count(x => x.behavior == Constants.eventShow)
    out.collect(Item(key, countBuy, countPv, window.getStart, window.getEnd).asInstanceOf[B])
  }
}
