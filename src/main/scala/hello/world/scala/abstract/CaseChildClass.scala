package hello.world.scala.`abstract`

case class CaseChildClass(name: String) extends AbstractClass(name) {
  override def printName: Unit = println(name)
}
