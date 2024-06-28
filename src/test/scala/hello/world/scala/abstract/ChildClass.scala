package hello.world.scala.`abstract`

class ChildClass(name: String) extends AbstractClass(name) {
  override def printName: Unit = println(name)
}

/**
  * singleton way to create Companion Object
  */
object ChildClass{
  def apply(name: String): AbstractClass = new ChildClass(name)
}
