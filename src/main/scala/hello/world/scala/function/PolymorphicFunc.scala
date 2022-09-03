package hello.world.scala.function

object PolymorphicFunc extends App{
  def applyDiscountWithReturnType[T](discount: T): Seq[T] = {
    discount match {
      case d: String =>
        println(s"Lookup percentage discount in database for $d")
        Seq[T](discount, discount, discount)
      case d: Double =>
        println(s"$d discount will be applied")
        Seq[T](discount, discount, discount)
      case d@_ =>
        println("Unsupported discount type")
        Seq[T](discount, discount, discount)
    }
  }

  println(s"Result of applyDiscountWithReturnType with Char parameter = ${applyDiscountWithReturnType[Char]('U')}")

}
