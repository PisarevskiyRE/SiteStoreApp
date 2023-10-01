package transforms

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.{Collector, OutputTag}
import scheme.{Item, OutputData, OutputItem}

class SplitItemsOutside[A <: Item, B <: OutputData](
                                  a: OutputTag[B],
                                  b: OutputTag[B]
                                  ) extends ProcessFunction[A, A]{
  override def processElement(
                               value: A,
                               ctx: ProcessFunction[A, A]#Context,
                               out: Collector[A]): Unit = {
    if ( value.bayCnt > 0 )
      ctx.output(a, OutputItem(value.itemId, value.bayCnt, value.from, value.to).asInstanceOf[B])

    if (value.showCnt > 0 )
      ctx.output(b, OutputItem(value.itemId, value.showCnt, value.from, value.to).asInstanceOf[B])

    out.collect(value)
  }
}
