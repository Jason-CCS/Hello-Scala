package hello.world.scala.function

object HighOrderFunc extends App {

  def itemTotalCost(item: String)(qty: Int)(price: Double)(qtyDiscount: Int => Double) = {
    println(s"You bought $qty $item, total cost is ${qty * price * qtyDiscount(qty)}")
  }

  def priceAfterDiscount(qty: Int): Double = qty match {
    case q if (q > 100) => 0.8
    case q if (q > 50) => 0.9
    case q if (q > 10) => 0.95
    case _ => 1.0
  }

  itemTotalCost("choco")(11)(100)(priceAfterDiscount)
}
