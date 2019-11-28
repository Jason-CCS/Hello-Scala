package hello.world.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile = "/Users/jasonchang/spark-2.4.3-bin-hadoop2.7/README.md" // Should be some file on your system
    val sc = new SparkContext(new SparkConf().setAppName("wordcount").setMaster("local"))
    val input = sc.textFile(logFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    counts.saveAsTextFile("./output")
  }
}
