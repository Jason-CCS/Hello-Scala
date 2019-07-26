package hello.world.scala.scala_test

class Donut(name: String, productCode: Option[Long] = None){

  def print = println(s"Donut name = $name, productCode = ${productCode.getOrElse(0)}, uuid=${Donut.uuid}")

}

object Donut{
  private val uuid =1
  def apply(name: String): Donut = {
    name match {
      case "Glazed Donut" => new GlazedDonut(name)
      case "Vanilla Donut" => new VanillaDonut(name)
      case _ => new Donut(name)
    }
  }
}

class GlazedDonut(name: String) extends Donut(name)

class VanillaDonut(name: String) extends Donut(name)


