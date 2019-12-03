package streaming

import java.nio.charset.Charset
import java.util
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

class ObjectDeserializer[T <: AnyRef](tType: Class[T]) extends Deserializer[T] {

  private val actualType = tType

  override def configure(props: util.Map[String, _], isKey: Boolean): Unit = {
  }

  override def deserialize(s: String, bytes: Array[Byte]): T = {

    if (bytes == null || bytes.length == 0) {
      return null.asInstanceOf[T]
    } else {
      try {
        val json = new String(bytes, Charset.forName("utf-8"))
        implicit val formats = DefaultFormats
        implicit val mf = Manifest.classType[T](actualType)
        val data = Serialization.read[T](json)(formats, mf)
        return data
      } catch {
        case ex: Exception =>
          throw new SerializationException(ex)
      }
    }
  }

  override def close(): Unit = {
  }
}