package transforms

import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentType
import scheme.{Item, OutputItem}

import scala.jdk.CollectionConverters.MapHasAsJava


class RequestIndex(indexName: String) {

  def createIndexRequest(element: OutputItem): IndexRequest = {

    val json = Map(
      "itemId" -> element.itemId.toString,
      "cnt" -> element.cnt.toString,
      "@timestamp" -> element.from.toString,
      "to" -> element.to.toString
    )

    Requests
      .indexRequest
      .index(indexName)
      .source(json.asJava, XContentType.JSON)
  }

  def createIndexRequest(element: Item): IndexRequest = {

    val json = Map(
      "itemId" -> element.itemId.toString,
      "bayCnt" -> element.bayCnt.toString,
      "showCnt" -> element.showCnt.toString,
      "@timestamp" -> element.from.toString,
      "to" -> element.to.toString
    )

    Requests
      .indexRequest
      .index(indexName)
      .source(json.asJava, XContentType.JSON)
  }
}
