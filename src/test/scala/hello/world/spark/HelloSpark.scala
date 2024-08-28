package hello.world.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.reflect.io.Directory

class HelloSpark extends AnyFunSuite with BeforeAndAfter {
  var spark: SparkSession = _
  var sc: SparkContext = _

  before {
    spark = SparkSession.builder.appName("Hello Spark").master("local[1]").getOrCreate()
    sc = spark.sparkContext
    spark.sparkContext.setLogLevel("WARN")
  }

  after {
    spark.close()
  }

  /**
   * RDD Basic Examples
   */
  test("wholeTextFiles") {
    val input = sc.wholeTextFiles("./input")
    // read all files under the specified folder, and use file name as key, file content as value
    println(input.collectAsMap())
  }

  test("word count demo") {
    val input = sc.textFile("input/news")
    val words = input.flatMap(line => line.split(" "))

    // map/reduce way to do word count
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    println(counts.collect().mkString("Array(", ", ", ")"))
    //    counts.saveAsTextFile("./output")
  }

  test("mapPartitions and groupByKey") {
    val gradesData = sc.textFile("./input/students_grades.csv")

    val studentGrades = gradesData.map(line => {
      val splitLine = line.split(",")
      (splitLine(0), splitLine(2).toDouble) // assuming the csv file doesn't contain a header
    })

    // I guess groupByKey is something frequently pair with mapPartitions
    val studentAvgGrades = studentGrades.groupByKey().mapPartitions {
      iter => {
        val res = ListBuffer[(String, Double)]()
        iter.foreach { case (student, grades) => {
          val sum = grades.sum
          val count = grades.size
          res += ((student, sum / count)) // double parenthesis is to wrap up as a tuple.
        }
        }
        res.iterator
      }
    }

    studentAvgGrades.collect().foreach(println(_))
  }

  test("union and intersection") {
    val visits_2023 = sc.textFile("input/visits_2023.csv")
    val visits_2024 = sc.textFile("input/visits_2024.csv")

    // Task 1: Get the intersection of both RDDs
    val commonVisits = visits_2023.intersection(visits_2024) // return distinct intersection only, see the definition of this function
    commonVisits.collect().foreach(println)

    // Task 2: Get the spots which became popular in 2024
    val newPopular = visits_2024.union(visits_2023).distinct().subtract(visits_2023.distinct())
    newPopular.collect().foreach(println)
  }

  test("join and reduceByKey") {
    val basket = List(("label1", "apple"), ("label2", "orange"), ("label3", "guava"), ("label1", "milk"), ("label3", "orange"))
    val price = List(("apple", 5), ("orange", 3), ("guava", 4), ("milk", 10))
    val basketRDD = sc.parallelize(basket)
    val priceRDD = sc.parallelize(price)
    val joinedRDD = basketRDD.map(t => t.swap).join(priceRDD) // the join here theoretically is a cartesian join
    println(joinedRDD.collect().mkString("Array(", ", ", ")"))
    val resultRDD = joinedRDD.map(t => (t._2._1, t._2._2)).reduceByKey((x, y) => x + y)
    val result = resultRDD.map(t => t._1 match {
      case "label1" => t._2 == 15
      case "label2" => t._2 == 3
      case "label3" => t._2 == 7
    }).reduce((x, y) => x | y)
    println(result)
  }

  test("aggregateByKey and reduceByKey") {
    val studentGrades = Array(("Alice", "Math", 85), ("Alice", "English", 92), ("Alice", "Physics", 78), ("Bob", "Math", 90), ("Bob", "English", 95), ("Bob", "Physics", 88))
    val rdd = sc.parallelize(studentGrades).cache
    // 1. Calculate the total sum of scores for each student across all subjects and store them in a RDD. The resultant RDD should have records in the format (studentName, totalScore).
    val studentScoreTotal = rdd.map(t => (t._1, t._3))
      .aggregateByKey(0)((acc, v) => acc + v, (x, y) => x + y).cache() // here seqOp is tricky. (U, V) => U, where U is the output type, V is the value type. V is not the whole (K, V) element in the previous RDD.
    studentScoreTotal.collect().foreach(println)

    // If you wanna use case (var1, var2, var3), you have to make sure the original input is only a type T. And you want to unwrap from T to case (var1, var2, var3).
    // This is my current assumption.

    // 2. Using the result from step 1, calculate the average score for each student and store them in another RDD. The resultant RDD should have records in the format (studentName, averageScore).
    val studentAvg = rdd.map(t => (t._1, 1)).reduceByKey(_ + _).join(studentScoreTotal).map {
      case (name, (subjectCount, scoreTotal)) => (name, scoreTotal.toDouble / subjectCount)
    }
    studentAvg.collect.foreach(println)
  }

  test("sortByKey") {
    val records = Array((123, ("Alice", 2)), (234, ("Bob", 1)), (345, ("Charlie", 5)), (456, ("Alice", 1)), (123, ("Charlie", 3)))
    val recordsRDD = sc.parallelize(records)
    val productTotalQuantity = recordsRDD.map(t => (t._1, t._2._2)).aggregateByKey(0)((acc, v) => acc + v, _ + _)
    productTotalQuantity.collect.foreach(println)

    productTotalQuantity.sortByKey().collect.foreach(println)
  }

  test("cogroup") {

  }

  test("cartesian") {}

  /**
   * Below are Personal practices.
   */
  test("Find the the anomaly Latte number which locates out of 3*stdev of normal distribution") {
    val orders = List(("Latte", 1), ("Latte", 2), ("Latte", 1), ("Latte", 1), ("Latte", 2), ("Latte", 2), ("Latte", 1), ("Latte", 3), ("Latte", 1), ("Latte", 2), ("Latte", 1), ("Latte", 11), ("Latte", 1), ("Latte", 1))
    println(s"How many orders: ${orders.size}")
    val ordersRDD = sc.parallelize(orders)
    val stat = ordersRDD.map(t => t._2.toDouble).stats()
    val outlier = ordersRDD.filter(t => math.abs(t._2 - stat.mean) > 3 * stat.stdev)
    println(s"Outliers count is ${outlier.count}. They are ${outlier.collect().mkString("Array(", ", ", ")")}")
  }

  /**
   * English alphabet influence rank.
   * To be honest, page rank is a high shuffling distributed processing.
   */
  test("Find the impact factor of each English alphabets") {
    val text = sc.textFile("input/news")
    val words = text.flatMap(line => line.split(" "))
      .filter(w => w.forall(_.isLetter)) // filter out words have non-letter
      .cache() // words will be reused again. cache it in worker.csv memory.
    println(s"we have ${words.count} words.")

    /**
     * calculate by page rank algorithm
     */
    val charToOtherChars = words.map(w => {
      val charList = w.toList // all the char in a list
      val firstChar = charList.head.toLower // use first char as key
      (firstChar, charList.filter(c => !c.toLower.equals(firstChar)).map(c => c.toLower))
    }) // eg. (a, [p, p, l, e])

    // associate all the dest chars with they first char as key. eg. (a, List(s, n, d, l, l, o, w, i, n, g, etc)), shuffling between executors.
    val links = charToOtherChars.reduceByKey((l1, l2) => l1 ::: l2).cache()
    println(links.collect.mkString("Array(", ", ", ")")) // collect(), get data from driver

    var ranks = sc.parallelize('a' to 'z').map((_, 1.0)) // default each char as 1.0 initial rank score.

    for (_ <- 1 to 10) {
      val contributions = links.join(ranks) // join by char a to z, need to associate the char with it's rank score. eg. (a, [p, p, l, e, x, e, etc], 1.5)
        .flatMap { // flatten the list of (char, contributed score=rank/cLinks.size)
          case (char, (cLinks, rank)) =>
            cLinks.map(destChar => (destChar, rank / cLinks.size)) // get the contributed score from rank for each destination char
        }

      ranks = contributions.reduceByKey((x, y) => x + y) // sum up all th contributed score from the same char key
        .mapValues(v => 0.15 + 0.85 * v) // re-distributed the score to avoid the score getting closely to zero
      println(s"ranks: ${ranks.collectAsMap()}")
    }

    deleteDirectory("output/p02")
    ranks.sortBy(_._2, ascending = false).saveAsTextFile("output/p02")
  }

  def deleteDirectory(dir: String): Unit = {
    val directory = new Directory(new File(dir))
    directory.deleteRecursively()
  }

  /**
   * important Dependency Injection.
   */
  test("use implicit ordering to control the order strategy") {
    val orders = List(("Latte", 1), ("Cappuccino", 2), ("Latte", 3), ("Americano", 1), ("Latte", 2), ("Cappuccino", 4))
    val ordersRDD = sc.parallelize(orders)
    ordersRDD.foreach(println(_)) // this is wrong way to print stuff. It will print in the executors. But here is local machine, so it's safe.

    implicit val sortByStringLen: Ordering[String] = new Ordering[String] {
      /**
       * return positive if x > y.
       */
      override def compare(x: String, y: String): Int = {
        x.length.compare(y.length)
      }
    }
    println(ordersRDD.sortByKey(ascending = false).collect().toList)
  }

  test("Coding: Workers With The Highest Salaries") {
    /**
     * You have been asked to find the job titles of the highest-paid employees.
     * Your output should include the highest-paid title or multiple titles with the same salary.
     */
    val worker = spark.read.option("header", "true").csv("input/worker.csv")
    val title = spark.read.option("header", "true").csv("input/title.csv")
    val joined = worker.join(title, worker("worker_id") === title("worker_ref_id")).cache()
    val top = joined.select(col("salary").cast("Int")).orderBy(desc("salary")).first().getInt(0)
    println(top)

    joined.filter(col("salary") === top).select("worker_title").show(false)
  }

  test("Coding: Salaries Differences") {
    /**
     * Write a query that calculates the difference between the highest salaries found in the marketing and engineering departments.
     * Output just the absolute difference in salaries.
     */
    val employees = spark.read.option("header", "true").csv("input/db_employee.csv")
    val depts = spark.read.option("header", "true").csv("input/db_dept.csv")
    val joinedDF = employees.join(depts, employees("department_id") === depts("id")).drop(depts("id")).cache()
    val maxInEngineering = joinedDF.filter(col("department") === "engineering").agg(max("salary")).head.getAs[String](0).toInt
    val maxInMarketing = joinedDF.filter(col("department") === "marketing").agg(max("salary")).head.getAs[String](0).toInt

    println(math.abs(maxInEngineering - maxInMarketing))
  }

  test("SQL: Salaries Differences") {
    /**
     * Write a query that calculates the difference between the highest salaries found in the marketing and engineering departments.
     * Output just the absolute difference in salaries.
     */
    spark.read.option("header", "true").csv("input/db_employee.csv").createOrReplaceTempView("employee")
    spark.read.option("header", "true").csv("input/db_dept.csv").createOrReplaceTempView("department")
    spark.sql(
      """
      SELECT ABS((SELECT MAX(salary) FROM employee JOIN department ON employee.department_id = department.id WHERE department = 'engineering') -
               (SELECT MAX(salary) FROM employee JOIN department ON employee.department_id = department.id WHERE department = 'marketing')) AS sal_diff
        """).show(false)
  }

  test("SQL: Users By Average Session Time") {
    /**
     * Calculate each user's average session time.
     * A session is defined as the time difference between a page_load and page_exit.
     * For simplicity, assume a user has only 1 session per day and if there are multiple of the same events on that day,
     * consider only the latest page_load and earliest page_exit,
     * with an obvious restriction that load time event should happen before exit time event.
     * Output the user_id and their average session time.
     */
    spark.read.option("header", "true").csv("input/facebook_web_log.csv").withColumn("timestamp", col("timestamp").cast("timestamp")).createOrReplaceTempView("facebook_web_log")
    spark.sql("select * from facebook_web_log").printSchema()
    spark.sql("select * from facebook_web_log").show(false)

    // Inner JOIN on user_id and date_only can retain non-null value only,
    // cause some user_id might have page_load_ts only but do not have page_exit_ts.
    // In this case, we will only have page_load_ts in table a, but won't have page_exit_ts in table b.
    // And if we join on user_id and date_only, a.date_only has value, but b.date_only is null.
    // INNER JOIN can filter out a.date_only != b.date_only
    val s1 = spark.sql(
      """
        |select a.user_id, avg(unix_timestamp(page_exit_ts) - unix_timestamp(page_load_ts)) as avg_seconds from
        |(select user_id, date(timestamp) as date_only, max(timestamp) as page_load_ts
        |from facebook_web_log
        |where action='page_load'
        |group by user_id, date(timestamp)) as a
        |inner join
        |(select user_id, date(timestamp) as date_only, min(timestamp) as page_exit_ts
        |from facebook_web_log
        |where action='page_exit'
        |group by user_id, date(timestamp)) as b
        |on a.user_id=b.user_id and a.date_only = b.date_only
        |where page_load_ts < page_exit_ts
        |group by a.user_id;
        |""".stripMargin)
    s1.printSchema()
    s1.show(false)

    // possible to have one query only without subquery? No
    // but possible to have no join as below.
    // This is more spark functional way.
    val s2 = spark.sql(
      """
        |SELECT user_id, avg(diff_seconds) as avg_seconds FROM
        |(SELECT user_id, (unix_timestamp(min(if(action='page_exit', timestamp, NULL)))-unix_timestamp(max(if(action='page_load', timestamp, NULL)))) AS diff_seconds
        |FROM facebook_web_log
        |GROUP BY user_id, date(timestamp))
        |WHERE diff_seconds is NOT NULL
        |GROUP BY user_id
        |""".stripMargin)
    s2.printSchema()
    s2.show(false)
  }

  test("SQL: Finding User Purchases") {
    /**
     * Write a query that'll identify returning active users.
     * A returning active user is a user that has made a second purchase within 7 days of any other of their purchases.
     * Output a list of user_ids of these returning active users.
     */
    spark.read.option("header", "true").csv("input/amazon_transactions.csv").withColumn("created_at", col("created_at").cast("date")).createOrReplaceTempView("transactions")
    spark.sql("select * from transactions").printSchema()
    spark.sql("select * from transactions").show(false)

    /**
     * Self Join use case.
     * When you want to pair your row of data with other rows, then you can do self join for pairing up.
     */
    val s1 = spark.sql(
      """
        |SELECT DISTINCT t1.user_id FROM
        |transactions AS t1
        |JOIN
        |transactions AS t2
        |ON t1.user_id = t2.user_id
        |WHERE t1.id != t2.id AND abs(date_diff(t1.created_at, t2.created_at)) BETWEEN 0 AND 7
        |ORDER BY t1.user_id
        |""".stripMargin)

    s1.printSchema()
    s1.show(false)
  }
}
