package transforms

import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.connector.elasticsearch.sink.{Elasticsearch7SinkBuilder, ElasticsearchSink, RequestIndexer}
import org.apache.http.HttpHost
import scheme._

class SinkElastic(host: HttpHost) {

  def getSink(indexName: String) = {

    val sink: ElasticsearchSink[OutputItem] = new Elasticsearch7SinkBuilder[OutputItem]()
      .setBulkFlushInterval(1000L)
      .setBulkFlushMaxActions(1)
      .setHosts(host)
      .setEmitter((element: OutputItem, context: SinkWriter.Context, indexer: RequestIndexer) =>
        indexer.add(
          new RequestIndex(indexName).createIndexRequest(element)))
      .build()

    sink
  }

  def getSinkFull(indexName: String) = {

    val sink: ElasticsearchSink[Item] = new Elasticsearch7SinkBuilder[Item]()
      .setBulkFlushInterval(1000L)
      .setBulkFlushMaxActions(1)
      .setHosts(host)
      .setEmitter((element: Item, context: SinkWriter.Context, indexer: RequestIndexer) =>
        indexer.add(
          new RequestIndex(indexName).createIndexRequest(element)))
      .build()

    sink
  }

}
