package hello.world.delta_lake

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.scalatest.funsuite.AnyFunSuite

class HelloDelta extends AnyFunSuite {
  val spark = SparkSession.builder.appName("Hello Delta Lake").master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val tableLocation = "./tmp/delta-table"

  test("test update") {
    val data = spark.range(0, 5)
    data.write.format("delta").mode("overwrite").save(tableLocation)
    val df = spark.read.format("delta").load(tableLocation)
    df.show()

    val data2 = spark.range(5, 10)
    data2.write.format("delta").mode("overwrite").save(tableLocation)
    val df2 = spark.read.format("delta").load(tableLocation)
    df2.show()
  }

  test("conditional update") {
    val deltaTable = DeltaTable.forPath(spark, tableLocation)

    // Update every even value by adding 100 to it
    deltaTable.update(condition = expr("id % 2 == 0"), set = Map("id" -> expr("id + 100")))

    spark.read.format("delta").load(tableLocation).show(false)

    // Delete every even value
    deltaTable.delete(condition = expr("id % 2 == 0"))

    spark.read.format("delta").load(tableLocation).show(false)

    // Upsert (merge) new data
    val newData = spark.range(0, 20).toDF

    deltaTable.as("oldData")
      .merge(newData.as("newData"), "oldData.id = newData.id")
      .whenMatched
      .update(Map("id" -> col("newData.id")))
      .whenNotMatched
      .insert(Map("id" -> col("newData.id")))
      .execute()

    deltaTable.toDF.show()
  }
}
