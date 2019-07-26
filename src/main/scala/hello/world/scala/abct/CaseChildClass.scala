package hello.world.scala.abct

case class CaseChildClass(name: String) extends AbstractClass(name) {
  override def printName: Unit = println(name)
}
