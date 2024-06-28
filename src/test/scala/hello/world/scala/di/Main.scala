package hello.world.scala.di

object Main {
  def main(args: Array[String]): Unit = {
    val donutShoppingCart: DonutShoppingCart[String] = new DonutShoppingCart[String]()
    donutShoppingCart.add("Vanilla Donut")
    donutShoppingCart.update("Vanilla Donut")
    donutShoppingCart.search("Vanilla Donut")
    donutShoppingCart.delete("Vanilla Donut")
    donutShoppingCart.checkStockQuantity("Vanilla Donut")
  }
}
