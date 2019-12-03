package streaming

import java.nio.charset.Charset
import java.util

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization


class ObjectSerializer[T <: AnyRef] extends Serializer[T] {

  override def configure(config: util.Map[String, _], isKey: Boolean): Unit = {
  }

  override def serialize(topic: String, data: T): Array[Byte] = {

    if (data == null) {
      return Array.empty[Byte]
    } else {
      try {
        implicit val formats = DefaultFormats
        val json = Serialization.write[T](data)
        val bytes = json.getBytes(Charset.forName("utf-8"))
        return bytes
      } catch {
        case ex: Exception =>
          throw new SerializationException(ex)
      }
    }
  }

  override def close(): Unit = {
  }
}