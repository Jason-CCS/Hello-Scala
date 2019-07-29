package hello.world.spark

import org.apache.spark.sql.SparkSession

object ParquetReader {
  def main(args: Array[String]): Unit = {
    val parquetPath = System.getProperty("user.dir") + System.getProperty("file.separator") + "/data/" +
      System.getProperty("file.separator") + "part-00000-95bbfcbb-ffbe-4112-8e69-876234860dd4-c000.snappy.parquet"
    println(parquetPath)
    val spark = SparkSession.builder.master("local").appName("Simple Application").getOrCreate()
    val df = spark.read.parquet(parquetPath)

    spark.stop()
  }

}
