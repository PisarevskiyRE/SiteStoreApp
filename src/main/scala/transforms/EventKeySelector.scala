package transforms

import org.apache.flink.api.java.functions.KeySelector
import scheme.RawData

class EventKeySelector[A <: RawData] extends KeySelector[A, Int] {
  override def getKey(value: A): Int = value.item_id

}
