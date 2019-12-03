package batch

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveToHbaseJob extends App{

  // create spark session
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("data-mover")
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


  val transactions = spark.sqlContext.sql("select * from transactions")


  import org.apache.spark.sql.expressions.UserDefinedFunction
  import org.apache.spark.sql.functions._


  val arr = Array("a","b","c","d","f")
  val r = scala.util.Random
  def myNextPositiveNumber :String = { arr(r.nextInt(5))}

  val f: UserDefinedFunction = udf(myNextPositiveNumber _)
  val fNonDeterministic: UserDefinedFunction = f.asNondeterministic

  val myNewDF = transactions.withColumn("id", fNonDeterministic())

  import spark.implicits._

  val result = myNewDF.select(concat($"id", lit("_"), $"idTransaction", lit("_"), $"idProduit").as("key"), $"idTransaction", $"idProduit",$"idMagasin", $"quantite", $"timestamp")


  result.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()
}
