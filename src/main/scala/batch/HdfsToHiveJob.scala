package batch

import org.apache.spark.sql.SparkSession

object HdfsToHiveJob extends App{

  // create spark session
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("data-mover")
    .enableHiveSupport()
    .getOrCreate()



  // create transactions table using hive
  val dropTable = spark.sqlContext.sql("DROP TABLE IF EXISTS transactions")
  val createTableHive = spark.sqlContext
    .sql("CREATE TABLE IF NOT EXISTS transactions (timestamp STRING, idProduit STRING, idTransaction STRING," +
      "idMagasin STRING, quantite STRING)")



  // Read in the parquet file from hdfs
  val parquetFileDF = spark.read.parquet("/tmp/data/")



  // Writing Dataframe as hive table transactions
  parquetFileDF.write.mode("overwrite").saveAsTable("transactions")

}
