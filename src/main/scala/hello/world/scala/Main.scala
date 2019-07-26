package hello.world.scala

object Main {
  def main(args: Array[String]): Unit = {
    val glazedDonut = Donut("Glazed Donut")
    println(s"The class type of glazedDonut = ${glazedDonut.getClass}")
    glazedDonut.print

    // call singleton obj
    println("\nStep 4: How to call global discount field from Step 2")
    println(s"Global discount = ${DonutShoppingCartCalculator.discount}")

    println("\nStep 5: How to call the utility function calculateTotalCost from Step 3")
    println(s"Call to calculateTotalCost function = ${DonutShoppingCartCalculator.calculateTotalCost(List())}")

    // string interpolation
    println()
    val donutName: String = "Vanilla Donut"
    val donutTasteLevel: String = "Tasty"
    println(f"$donutName%20s $donutTasteLevel")
    println()
    println(raw"\tprint raw string \b \t")

    //
  }


}

