package hello.world.scala.abct

object Main {
  def main(args: Array[String]): Unit = {
    val glazedDonut: AbstractClass = ChildClass("glazed donut")
    val vanillaDonut: AbstractClass = ChildClass("vanilla donut")

    glazedDonut.printName
    vanillaDonut.printName
  }
}
