package batch

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._


object HbaseToElasticJob extends App {

  // create spark session
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("data-mover")
    .config("spark.es.nodes","127.0.0.1")
    .config("spark.es.port","9200")
    .config("spark.es.nodes.wan.only","true")
    .enableHiveSupport()
    .getOrCreate()



   def catalog = s"""{
                   |"table":{"namespace":"default", "name":"hbase_transactions"},
                   |"rowkey":"key",
                   |"columns":{
                   |"key":{"cf":"rowkey", "col":"key", "type":"string"},
                   |"idTransaction":{"cf":"tr", "col":"idTransaction", "type":"string"},
                   |"idProduit":{"cf":"tr", "col":"idProduit", "type":"string"},
                   |"idMagasin":{"cf":"tr", "col":"idMagasin", "type":"string"},
                   |"quantite":{"cf":"tr", "col":"quantite", "type":"string"},
                   |"timestamp":{"cf":"tr", "col":"timestamp", "type":"string"}
                   |}
                   |}""".stripMargin




  def withCatalog(cat: String): DataFrame = {
    spark.sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat
      ))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  val df = withCatalog(catalog)
  println("number of hbase dataframe partitions is: " + df.rdd.getNumPartitions)


  df.saveToEs("data-mover/transactions")
}
