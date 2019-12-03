package streaming

import org.apache.spark.sql.SparkSession
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.streaming.kafka010._



object KafkaToElasticJob extends App {

  // create spark session
  val sparkSess = SparkSession.builder()
    .master("local[4]")
    .appName("data-mover")
    .getOrCreate()

  val sc = sparkSess.sparkContext
  val ssc = new StreamingContext(sc, Seconds(1))


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "sandbox-hdp.hortonworks.com:6667",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[ProductDeserializer],
    "group.id" -> "elastic-group-consumer",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )


  val topics = Array("text_topic")
  val stream = KafkaUtils.createDirectStream[String, Product](
    ssc,
    PreferConsistent,
    Subscribe[String, Product](topics, kafkaParams)
  )

  val messages = stream.map(record => record.value)
  messages.foreachRDD({ rdd =>
    val products = sparkSess.sqlContext.createDataFrame(rdd).toDF("idProduit","prixUnitaire","quantiteDisponible","idMagasin")
    products.show()

    if (!products.head(1).isEmpty){
      //Index the Word, Count attributes to ElasticSearch Index. You don't need to create any index in Elastic Search
      import org.elasticsearch.spark.sql._
      products.saveToEs("kafka-data-mover/products")
    }

  })

  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate
}
