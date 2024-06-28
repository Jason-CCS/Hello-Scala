package hello.world.scala.scala_test

import scala.concurrent.{CanAwait, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.Try

class DonutStore[A] extends DonutDatabase[A] {
  override def addOrUpdate(donut: A): Long = {
    println(s"CassandraDonutDatabase-> addOrUpdate method -> donut: $donut")
    1
  }

  override def query(donut: A): A = {
    println(s"CassandraDonutDatabase-> query method -> donut: $donut")
    donut
  }

  override def delete(donut: A): Boolean = {
    println(s"CassandraDonutDatabase-> delete method -> donut: $donut")
    true
  }

  def favoriteDonut():String = "vanilla donut"

  def donuts():Seq[String] = Seq("vanilla donut", "plain donut", "glazed donut")

  def printName(): Unit = {
    throw new IllegalStateException("Some Error")
  }

  def donutPrice(donut: String): Option[Double] = {
    val prices = Map(
      "vanilla donut" -> 2.0,
      "plain donut"   -> 1.0,
      "glazed donut"  -> 3.0
    )
    val priceOfDonut = prices.get(donut)
    priceOfDonut.map{ price => price * (1 - discountByDonut(donut)) }
  }

  private def discountByDonut(donut: String): Double = {
    val discounts = Map(
      "vanilla donut" -> 0.2,
      "plain donut"   -> 0.1,
      "glazed donut"  -> 0.3
    )
    discounts.getOrElse(donut, 0)
  }

}
