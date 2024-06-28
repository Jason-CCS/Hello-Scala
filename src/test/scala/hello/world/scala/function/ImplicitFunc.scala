package hello.world.scala.function

import scala.language.implicitConversions

object ImplicitFunc extends App{

  class DonutString(s: String) {
    def isFavoriteDonut: Boolean = s == "Glazed Donut"
  }

  object DonutConversion {
    implicit def stringToDonutString(s: String): DonutString = new DonutString(s)
  }

  import DonutConversion._
  val glazedDonut = "Glazed Donut"
  val vanillaDonut = "Vanilla Donut"

  println(s"Is Glazed Donut my favorite donut? ${glazedDonut.isFavoriteDonut}")
  println(s"Is Vanilla Donut my favorite donut? ${vanillaDonut.isFavoriteDonut}")

}
