package hello.world.scala.pattern.matching

object Main extends App {
  val donutType = "Glazed Donut"
  val tasteLevel = donutType match {
    case donut if (donut.contains("Glazed") || donut.contains("Strawberry")) => "Very tasty"
    case "Plain Donut" => "Tasty"
    case _ => "Tasty"
  }

  val priceOfDonut: Any = 2.50
  val priceType = priceOfDonut match {
    case price: Int => "Int"
    case price: Double => "Double"
    case price: Float => "Float"
    case price: String => "String"
    case price: Boolean => "Boolean"
    case price: Char => "Char"
    case price: Long => "Long"
  }

  println(s"$donutType is $tasteLevel")
  println(s"Donut price type = $priceType")

}
