package hello.world.scala.collection.function

object Main {
  def main(args: Array[String]): Unit = {
    println("element order in set")
    println(Set("a", "b", "c"))
    println(Set("b", "c", "a"))

    println(List("a", "b", "c"))
    println(List("b", "c", "a"))

    println(Seq("a", "b", "c"))
    println(Seq("b", "c", "a"))

    println("union()")
    println(donuts.union(drink))
    println(donuts ++ drink)

    println("take()")
    println(donuts.take(1))
    println(donuts.take(2))
    println(donuts.take(3) == donuts.take(4))

    println("collect()")
    println("collect the donut which has e letter.")
    println(donuts.collect { case donut if donut.contains("e") => donut })
    val predicate: PartialFunction[String, String] = {
      case t if t.contains("e") => t
      case "Milk" => "Milk"
    }
    println("collect the thing having letter e or is Milk")
    println(donuts.union(drink).collect(predicate))

    println("map()")
    println(donuts.map(_ + "donut"))

    println("flatMap()")
    println(Set(donuts, drink))
    println(donuts.flatMap( d => d.split("a")))

  }

  val donuts: Set[String] = Set("Plain", "Glazed", "Strawberry")
  val drink: Set[String] = Set("Tea", "Coffee", "Milk")
}
