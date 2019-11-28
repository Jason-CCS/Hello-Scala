package hello.world.scala.function

object Main {
  def main(args: Array[String]): Unit = {
    println("partial function, make pattern matching => function")
    println(predicate("I have e"))
    println(predicate("AAAA"))

  }

  val donuts1: Seq[String] = Seq("Plain", "Strawberry", "Glazed")

  val predicate: PartialFunction[String, String] = {
    case t if t.contains("e") => t
    case "Milk" => "Milk"
    case _ => "no match"
  }


}
