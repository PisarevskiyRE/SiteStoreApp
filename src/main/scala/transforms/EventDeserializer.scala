package transforms

import io.circe.parser.decode
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import scheme.{Constants, Event}

import java.nio.charset.StandardCharsets

class EventDeserializer extends DeserializationSchema[Event] {

  import Event.decoder

  override def deserialize(message: Array[Byte]): Event = {

    val jsonString = new String(message, StandardCharsets.UTF_8)

    decode[Event](jsonString) match {
      case Right(event) => event
      case Left(error) => throw new RuntimeException(s"${Constants.errorSerializeLabal} : $error")
    }

  }

  override def isEndOfStream(nextElement: Event): Boolean = false

  implicit val typeInfo = TypeInformation.of(classOf[Event])

  override def getProducedType: TypeInformation[Event] = implicitly[TypeInformation[Event]]
}
