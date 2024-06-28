package hello.world.scala.traits

trait DonutInventoryService[A] {
  def checkStockQuantity(donut: A): Int
}
