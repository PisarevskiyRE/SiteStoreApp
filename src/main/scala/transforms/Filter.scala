package transforms

import org.apache.flink.api.common.functions.FilterFunction
import scheme.{Event, RawData}

class FilterEvents[A <: RawData](
                    events: Seq[String]
                  ) extends FilterFunction[A]{
  override def filter(value: A): Boolean = {
    events.contains(value.behavior)
  }
}
