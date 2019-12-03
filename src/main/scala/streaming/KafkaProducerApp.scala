package streaming

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object KafkaProducerApp extends App {

  val props:Properties = new Properties()
  props.put("bootstrap.servers","sandbox-hdp.hortonworks.com:6667")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", classOf[ObjectSerializer[Product]])
  props.put("acks","all")
  props.put("buffer.memory","52428800")
  props.put("batch.size","52428800")
  props.put("linger.ms","100")


  //create producer instance
  val producer = new KafkaProducer[String, Product](props)
  val topic = "text_topic"


  try {

    while (true) {
      val product = ProductGenerator.generateProduct()
      val key = product.idProduit + "-" + product.idMagasin
      val record = new ProducerRecord[String, Product](topic, key, product)
      val metadata = producer.send(record)
      printf(s"sent record(key=%s value=%s) " +
       "meta(partition=%d, offset=%d)\n",
       record.key(), record.value(),
       metadata.get().partition(),
       metadata.get().offset())
    }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      producer.close()
    }
}






