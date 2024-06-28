package hello.world.scala.string

object String2 extends App {
  println("\nTip: stripMargin using a different character")
  val donutJson5: String =
    """
    #{
    #"donut_name":"Glazed Donut",
    #"taste_level":"Very Tasty",
    #"price":2.50
    #}
    """.stripMargin('#')
  println(s"donutJson5 = $donutJson5")
}
