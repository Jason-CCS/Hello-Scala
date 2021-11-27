package hello.world.spark

import java.util

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object ParquetReader {

  //  if (System.getProperty("os.name").contains("Windows")) {
  //    System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\src\\main\\resources\\hadoop")
  //  }
  //  System.out.println(System.getProperty("hadoop.home.dir"))
  val spark = SparkSession.builder.master("local[1]").appName("Simple Application").getOrCreate()
  var df: DataFrame = _

  def main(args: Array[String]): Unit = {

    df = spark.read.parquet("")
    df.show(1,false)

    spark.stop()
  }

  def getAsRowList(): util.List[Row] = {
    df.takeAsList(df.count().toInt)
//    val arrayRow = df.collect()
  }

  def getColumns(): Array[String] = {
    df.columns
  }
}
