package transforms

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import scheme.Event

class Watermark(ofMillis: Long) {

  def getWatermarkStrategy[A <: Event](): WatermarkStrategy[A] = WatermarkStrategy
    .forBoundedOutOfOrderness(java.time.Duration.ofMillis(ofMillis))
    .withTimestampAssigner(new SerializableTimestampAssigner[A] {
      override def extractTimestamp(element: A, recordTimestamp: Long): Long = {
        element.ts.toEpochMilli
      }
    })
}
