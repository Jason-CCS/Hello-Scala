package hello.world.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]): Unit = {
    test2()
  }

  def solution(rdd: RDD[Int], m: Int, n: Int): Array[RDD[Int]] = {
    // your code start here
    val count = rdd.count()
    val myPart = rdd.filter(i => (i < count * m / (m + n)))
    val herPart = rdd.filter(i => (i > count * m / (m + n)))
    Array(myPart, herPart)
  }

  def test1(): Unit = {
    val numbers = Range(1, 501)
    val sc = new SparkContext(new SparkConf().setAppName("spark app").setMaster("local"))
    val rdd = sc.parallelize(numbers)
    rdd.collect().foreach(println(_))
    val m = 2
    val n = 3
    val output = SimpleApp.solution(rdd, m, n)
    if (output(0).take(5).sameElements(Array(1, 2, 3, 4, 5)) & output(1).take(5).sameElements(Array(201, 202, 203, 204, 205))) print("true")
    else print("false")
  }

  def solution2(rdd1:RDD[(String, String)], rdd2:RDD[(String, Int)]) = {
    rdd1.map(t=>t.swap).join(rdd2).map(t => (t._2._1, t._2._2)).reduceByKey((x, y) => x+y)
  }

  def test2(): Unit = {
    val basket = List(("label1", "apple"), ("label2", "orange"), ("label3", "guava"), ("label1", "milk"), ("label3", "orange"))
    val price = List(("apple", 5), ("orange", 3), ("guava", 4), ("milk", 10))
    val sc = new SparkContext(new SparkConf().setAppName("spark app").setMaster("local"))
    val basketRDD = sc.parallelize(basket)
    val priceRDD = sc.parallelize(price)
    val resultRDD = SimpleApp.solution2(basketRDD, priceRDD)
    val result = resultRDD.map(t => t._1 match {
      case "label1" => t._2 == 15
      case "label2" => t._2 == 3
      case "label3" => t._2 == 7
    }).reduce((x, y) => x|y)
    print(result)
  }

  /**
   * the example to demo how to remove outliers order
   * ("Latte", num): num is the number sold in each order
   */
  def removeOutliers = {
    val sc = new SparkContext(new SparkConf().setAppName("wholeTextFiles").setMaster("local"))
    val orders = List(("Latte", 1), ("Latte", 2), ("Latte", 1), ("Latte", 1), ("Latte", 2), ("Latte", 2), ("Latte", 1), ("Latte", 3), ("Latte", 1), ("Latte", 2), ("Latte", 1), ("Latte", 11), ("Latte", 1), ("Latte", 1))
    val ordersRDD = sc.parallelize(orders)
    val stat = ordersRDD.map(t => t._2.toDouble).stats()
    val reasonableOrdersRDD = ordersRDD.filter(t => {
      math.abs(t._2 - stat.mean) < 3 * stat.stdev
    })
    println(orders.size)
    println(reasonableOrdersRDD.count())

  }

  /**
   * sc.wholeTextFiles will read all files under the specified folder, and use file name as key, file content as value
   */
  def wholeTextFiles = {
    val sc = new SparkContext(new SparkConf().setAppName("wholeTextFiles").setMaster("local"))
    val input = sc.wholeTextFiles("./input")
    println(input.collectAsMap())
  }



  /**
   * English alphabet influence rank
   */
  def alphabetInfluenceRank = {
    val symbolList = Set('’', '”', '…', ';', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '-', '=', '+', ':', ':', '?', '<', '>', '[', ']', '"', '.', '\'', ',', '»', '–', '“')
    val sc = new SparkContext(new SparkConf().setAppName("wordcount").setMaster("local"))
    val text = sc.textFile("input/news")
    val words = text.flatMap(line => line.split(" "))
    println(words.count)

    /**
     * calculate by page rank algorithm
     */
    val charToOtherChars = words.map(w => (w.toList.head.toLower, w.toList.filter(c => !c.equals(w.toList.head)).map(c => c.toLower)))
    println(charToOtherChars.count)
    val links = charToOtherChars.filter(t => (!t._1.isDigit) & (!symbolList.contains(t._1)))
      .reduceByKey((l1, l2) => l1 ::: l2).map(t => (t._1, t._2.filterNot(symbolList)))
    //    println(links.count())

    var ranks = links.mapValues(v => 1.0)

    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (char, (cLinks, rank)) =>
          cLinks.map(dest => (dest, rank / cLinks.size))
      }
      //      println(contributions.collect().toList)
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
      //      print(ranks.collectAsMap())
    }

    ranks.sortBy(_._2).saveAsTextFile("output/p02")

    /**
     * calculate by letter count
     */
    val chars = words.flatMap(w => w.toList.filter(c => !symbolList.contains(c)).filter(c => !c.isDigit)).map(c => c.toLower)
    val charCounts = chars.map(c => (c, 1)).reduceByKey((x, y) => x + y)
    charCounts.sortBy(_._2).saveAsTextFile("output/c02")
  }

  def p3 = {
    val sc = new SparkContext(new SparkConf().setAppName("wordcount").setMaster("local"))
    val orders = List(("Latte", 1), ("Cupuccino", 2), ("Latte", 3), ("Choco", 1), ("Latte", 2), ("Cupuccino", 4))
    val ordersRDD = sc.parallelize(orders)
    ordersRDD.foreach(println(_))
    //    ordersRDD.sortByKey(true).foreach(println(_))
    implicit val sortByStringLen: Ordering[String] = new Ordering[String] {
      override def compare(x: String, y: String): Int = {
        x.length.compare(y.length)
      }
    }
    println(ordersRDD.sortByKey().collect().toList)
  }

  /**
   * print out the RDD order by self-defined function sortByStringLen
   */
  def p2 = {
    val sc = new SparkContext(new SparkConf().setAppName("wordcount").setMaster("local"))
    val orders = List(("Latte", 1), ("Cupuccino", 2), ("Latte", 3), ("Choco", 1), ("Latte", 2), ("Cupuccino", 4))
    val ordersRDD = sc.parallelize(orders)
    ordersRDD.foreach(println(_))
    //    ordersRDD.sortByKey(true).foreach(println(_))
    implicit val sortByStringLen: Ordering[String] = new Ordering[String] {
      override def compare(x: String, y: String): Int = {
        x.length.compare(y.length)
      }
    }
    println(ordersRDD.sortByKey().collect().toList)
  }

  /**
   * other practices
   * Pair RDD
   */
  def p1 = {

    val sc = new SparkContext(new SparkConf().setAppName("wordcount").setMaster("local"))
    val orders = List(("Latte", 1), ("Cupuccino", 2), ("Latte", 3), ("Choco", 1), ("Latte", 2), ("Cupuccino", 4))
    // put own data into spark
    val ordersRDD = sc.parallelize(orders)
    // practice: PairRDD.combineByKey
    val map = ordersRDD.combineByKey(
      v => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map { case (key, value) => (key, value._1 / value._2.toFloat) }.collectAsMap()
    println(map)
    // print out RDD
    println(ordersRDD.collect().toList)
  }

  /**
   * word count
   */
  def wordCount = {
    val logFile = "/Users/jasonchang/spark-2.4.3-bin-hadoop2.7/README.md" // Should be some file on your system
    val sc = new SparkContext(new SparkConf().setAppName("wordcount").setMaster("local"))
    val input = sc.textFile(logFile)
    val words = input.flatMap(line => line.split(" "))

    // map/reduce way to do word count
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    //    counts.saveAsTextFile("./output")

    // countByValue() way to do word count
    println(input.flatMap(line => line.split(" ")).countByValue())
  }
}
