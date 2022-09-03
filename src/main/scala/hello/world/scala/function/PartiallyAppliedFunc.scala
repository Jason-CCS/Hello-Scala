package hello.world.scala.function

object PartiallyAppliedFunc extends App {
  def totalCost(donutType: String)(quantity: Int)(discount: Double): Double = {
    println(s"Calculating total cost for $quantity $donutType with ${discount * 100}% discount")
    val totalCost = 2.50 * quantity
    totalCost - (totalCost * discount)
  }

  val totalCostForGlazedDonuts = totalCost("Glazed Donut") _

  print(s"${totalCostForGlazedDonuts(10)(0.1)}")
}
