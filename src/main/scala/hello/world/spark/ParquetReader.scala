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

    df = spark.read.parquet("E:\\writable\\OneDrive\\OneDrive - Emdeon\\Projects\\Hello-Scala\\output-data\\aetna\\part-00000-30e0dfa4-1474-4a63-9560-37ff9afc4328-c000.snappy.parquet")
    df.show(1,false)
    //    df = spark.read.parquet("E:\\writable\\OneDrive\\OneDrive - Emdeon\\Projects\\ihdp-anthem-to-parquet-pipeline\\processed\\document")
    //    df.show(false)
    //    println("-----------exception-------------")
    //    df = spark.read.parquet("E:\\writable\\OneDrive\\OneDrive - Emdeon\\Projects\\ihdp-anthem-to-parquet-pipeline\\processed\\exception")
    //    df.show(false)
    //    println("-----------file-------------")
    //    df = spark.read.parquet("E:\\writable\\OneDrive\\OneDrive - Emdeon\\Projects\\ihdp-anthem-to-parquet-pipeline\\processed\\file")
    //    df.show(false)
    //    println("-----------organization-------------")
    //    df = spark.read.parquet("E:\\writable\\OneDrive\\OneDrive - Emdeon\\Projects\\ihdp-anthem-to-parquet-pipeline\\processed\\organization")
    //    df.show(false)
    //    println("-----------person-------------")
    //    df = spark.read.parquet("E:\\writable\\OneDrive\\OneDrive - Emdeon\\Projects\\ihdp-anthem-to-parquet-pipeline\\processed\\person")
    //    df.show(false)
    //    println("-----------problem_observation-------------")
    //    df = spark.read.parquet("E:\\writable\\OneDrive\\OneDrive - Emdeon\\Projects\\ihdp-anthem-to-parquet-pipeline\\processed\\problem_observation")
    //    df.show(false)
    //    println("-----------procedure-------------")
    //    df = spark.read.parquet("E:\\writable\\OneDrive\\OneDrive - Emdeon\\Projects\\ihdp-anthem-to-parquet-pipeline\\processed\\procedure")
    //    df.show(false)
    //    println("-----------provider-------------")
    //    df = spark.read.parquet("E:\\writable\\OneDrive\\OneDrive - Emdeon\\Projects\\ihdp-anthem-to-parquet-pipeline\\processed\\provider")
    //    df.show(false)
    //    println("-----------service_line-------------")
    //    df = spark.read.parquet("E:\\writable\\OneDrive\\OneDrive - Emdeon\\Projects\\ihdp-anthem-to-parquet-pipeline\\processed\\service_line")
    //    df.show(false)
    //    println("-----------service_location-------------")
    //    df = spark.read.parquet("E:\\writable\\OneDrive\\OneDrive - Emdeon\\Projects\\ihdp-anthem-to-parquet-pipeline\\processed\\service_location")
    //    df.show(false)
    //    println("-----------subscriber-------------")
    //    df = spark.read.parquet("E:\\writable\\OneDrive\\OneDrive - Emdeon\\Projects\\ihdp-anthem-to-parquet-pipeline\\processed\\subscriber")
    //    df.show(false)

    //    println(df.count())
    spark.stop()
  }

  def getAsRowList(): util.List[Row] = {
    df.takeAsList(df.count().toInt)
  }

  def getColumns(): Array[String] = {
    df.columns
  }
}
