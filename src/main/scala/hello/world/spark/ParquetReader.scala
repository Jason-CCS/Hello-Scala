package hello.world.spark

import org.apache.spark.sql.SparkSession

object ParquetReader {
  def main(args: Array[String]): Unit = {
    val parquet = "/Users/jasonchang/spark-2.4.3-bin-hadoop2.7/data/" +
      "part-00000-95bbfcbb-ffbe-4112-8e69-876234860dd4-c000.snappy.parquet"
    val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()
    val in = spark.read.parquet(parquet)

    spark.stop()
  }

}
