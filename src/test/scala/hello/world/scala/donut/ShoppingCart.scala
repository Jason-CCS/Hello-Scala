package hello.world.scala.donut

class ShoppingCart[D <: Donut](donuts: Seq[D]) {

  def printCartItems: Unit = donuts.foreach(_.print)

}

/**
  * extends generic
  * @param donuts
  * @tparam D
  */
class ShoppingCart2[+D <: Donut](donuts: Seq[D]) {

  def printCartItems: Unit = donuts.foreach(_.print)

  def usage: Unit = {
    val shoppingCart2: ShoppingCart2[Donut] = new ShoppingCart2[VanillaDonut](Seq(new VanillaDonut("vanilla")))
    shoppingCart2.printCartItems
  }

}

/**
  * super generic
  * @param donuts
  * @tparam D
  */
class ShoppingCart3[-D <: Donut](donuts: Seq[D]) {

  def printCartItems: Unit = donuts.foreach(_.print)

  def usage: Unit = {
    val shoppingCart3: ShoppingCart3[VanillaDonut] = new ShoppingCart3[Donut](Seq(new GlazedDonut("glazed")))
  }
}