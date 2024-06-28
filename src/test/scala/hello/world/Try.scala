package hello.world

object Try extends App {
  def getV(): Option[String] = {
    Some("abc")
  }

  println(getV().get.padTo(10, "f"))

}
