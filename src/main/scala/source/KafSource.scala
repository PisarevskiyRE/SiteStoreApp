package source

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

import scheme.RawData


class KafSource[A <: RawData, B <: DeserializationSchema[A]](
                                 host: String,
                                 deserializer: B) {

  def getSource(topic: String):KafkaSource[A] = {
    val kafkaSource: KafkaSource[A] = KafkaSource.builder[A]()
      .setBootstrapServers(host)
      .setTopics(topic)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(deserializer)
      .build()
    kafkaSource
  }
}
