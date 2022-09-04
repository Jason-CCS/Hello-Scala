package hello.world.scala.`abstract`

object Main {
  def main(args: Array[String]): Unit = {
    val glazedDonut: AbstractClass = ChildClass("glazed donut")
    val vanillaDonut: AbstractClass = ChildClass("vanilla donut")
    val donut: CaseChildClass = CaseChildClass("lalala donut")

    glazedDonut.printName
    vanillaDonut.printName
    donut.printName
  }
}
