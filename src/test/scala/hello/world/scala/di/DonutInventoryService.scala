package hello.world.scala.di

trait DonutInventoryService[A] {
  val donutDatabase: DonutDatabase[A]

  def checkStockQuantity(donut: A): Int = {
    println(s"DonutInventoryService-> checkStockQuantity method -> donut: $donut")
    donutDatabase.query(donut)
    1
  }
}
