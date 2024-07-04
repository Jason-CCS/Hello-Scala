package hello.world.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.io.Directory

class Practice extends AnyFunSuite {
  val spark = SparkSession.builder.appName("Hello Spark").master("local[1]").getOrCreate()
  val sc = spark.sparkContext
  val config = new SparkConf

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

  test("intersection") {
    val visits_2023 = sc.textFile("input/visits_2023.csv")
    val visits_2024 = sc.textFile("input/visits_2024.csv")

    // Task 1: Get the intersection of both RDDs
    val commonVisits = visits_2023.intersection(visits_2024) // return distinct intersection only, see the definition of this function
    commonVisits.collect().foreach(println)

    // Task 2: Get the spots which became popular in 2024
    val newPopular = visits_2024.union(visits_2023).distinct().subtract(visits_2023.distinct())
    newPopular.collect().foreach(println)
  }

  test("mapPartitions") {
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

  test("join, test if the total amount of basket is equal to 15, 3, 7.") {
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

  test("aggregateByKey") {
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

  test("Write a query that calculates the difference between the highest salaries found in the marketing and engineering departments. Output just the absolute difference in salaries.") {
    val employees = spark.read.option("header", "true").csv("input/db_employee.csv")
    val depts = spark.read.option("header", "true").csv("input/db_dept.csv")
    val joinedDF = employees.join(depts, employees("department_id") === depts("id")).drop(depts("id")).cache()
    val maxInEngineering = joinedDF.filter(col("department") === "engineering").agg(max("salary")).head.getAs[String](0).toInt
    val maxInMarketing = joinedDF.filter(col("department") === "marketing").agg(max("salary")).head.getAs[String](0).toInt

    println(math.abs(maxInEngineering - maxInMarketing))
  }

  test("Your output should include the highest-paid title or multiple titles with the same salary.") {
    val worker = spark.read.option("header", "true").csv("input/worker.csv")
    val title = spark.read.option("header", "true").csv("input/title.csv")
    val joined = worker.join(title, worker("worker_id") === title("worker_ref_id")).cache()
    val top = joined.select(col("salary").cast("Int")).first().getInt(0)
    println(top)

    joined.filter(col("salary") === top).select("worker_title").show(false)
  }

  case class MachineAction(machine: String, state: String, epochTime: Long)

  case class StateChangeHistory(machine: String, startState: String, endState: String, startEpochTime: Long, endEpochTime: Long)

  def calculate(logs: List[MachineAction]): List[StateChangeHistory] = {
    val resultList = ListBuffer[StateChangeHistory]()

    logs.foldLeft(Map.empty[String, (StateChangeHistory, Int)]) { case (map, record) =>
      val currentState = record.state
      if (!map.contains(record.machine)) {
        val newHistory = StateChangeHistory(record.machine, currentState, "", record.epochTime, 0)
        resultList += newHistory
        map + (record.machine -> (newHistory, resultList.size - 1))
      } else {
        val oldHistory = map(record.machine)._1
        val oldIndex = map(record.machine)._2
        if (oldHistory.endState.isEmpty) { // if endState is empty, update the existing history record
          if (oldHistory.startState != currentState) {
            val newHistory = oldHistory.copy(endState = record.state, endEpochTime = record.epochTime)
            resultList(oldIndex) = newHistory
            val placeholderHistory = newHistory.copy(startState = newHistory.endState, endState = "", startEpochTime = newHistory.endEpochTime, endEpochTime = 0)
            resultList += placeholderHistory
            map.updated(record.machine, (placeholderHistory, resultList.size - 1))
          } else map
        } else { // if endState is not empty, add one more history record
          if (oldHistory.endState != currentState) {
            val newHistory = oldHistory.copy(startState = oldHistory.endState, endState = record.state, startEpochTime = oldHistory.endEpochTime, endEpochTime = record.epochTime) // clone
            resultList += newHistory
            val placeholderHistory = newHistory.copy(startState = newHistory.endState, endState = "", startEpochTime = newHistory.endEpochTime, endEpochTime = 0)
            resultList += placeholderHistory
            map.updated(record.machine, (placeholderHistory, resultList.size - 1))
          } else map
        }
      }
    }
    resultList.toList
  }

  test("test calculate") {
    // please generate the test cases for the function calculate like the below examples
    val logs = List(
      MachineAction("M1", "IDLE", 1800),
      MachineAction("M2", "IDLE", 1801),
      MachineAction("M2", "Running", 1802),
      MachineAction("M3", "IDLE", 1803),
      MachineAction("M4", "IDLE", 1804),
      MachineAction("M5", "IDLE", 1805),
      MachineAction("M1", "RUNNING", 1806),
      MachineAction("M2", "Stopping", 1807),
      MachineAction("M3", "RUNNING", 1808),
      MachineAction("M4", "IDLE", 1809),
      MachineAction("M5", "RUNNING", 1810)
    )
    calculate(logs).foreach(println)
  }
}
