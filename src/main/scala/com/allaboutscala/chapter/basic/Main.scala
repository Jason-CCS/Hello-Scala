package com.allaboutscala.chapter.basic

object Main {
  def main(args: Array[String]): Unit = {
    // string interpolation
    val donutName: String = "Vanilla Donut"
    val donutTasteLevel: String = "Tasty"
    println(s"donut name: $donutName")
    println("--------------")

    // string interpolation evaluation
    println(s"Is $donutName tasty? ${donutTasteLevel.equals("Tasty")}")
    println("--------------")

    // formatted string
    println(f"$donutName%20s $donutTasteLevel")
    println("--------------")


    // raw string
    println(raw"\tprint raw string \b \t")
    println("--------------")

    // multi-line string
    val jsonString =
      """
        |{
        |"donut_name":"Glazed Donut",
        |"taste_level":"Very Tasty",
        |"price":2.50
        |}
      """.stripMargin
    println(jsonString)
    println("--------------")

    // for loop with 2D array, note "until" there, we can use "to" for till meaning.
    val twoDimensionalArray = Array.ofDim[String](2, 2)
    twoDimensionalArray(0)(0) = "flour"
    twoDimensionalArray(0)(1) = "sugar"
    twoDimensionalArray(1)(0) = "egg"
    twoDimensionalArray(1)(1) = "syrup"
    for {x <- 0 until 2
         y <- 0 until 2
    } println(s"Donut ingredient at index ${(x, y)} = ${twoDimensionalArray(x)(y)}")
    println("--------------")

    //Learn How To Use Range
    println("Storing our ranges into collections")
    val listFrom1To5 = (1 to 5).toList
    println(s"Range to list = $listFrom1To5")

    val setFrom1To5 = (1 to 5).toSet
    println(s"Range to set = $setFrom1To5")

    val sequenceFrom1To5 = (1 to 5).toSeq
    println(s"Range to sequence = $sequenceFrom1To5")

    val arrayFrom1To5 = (1 to 5).toArray
    println(s"Range to array = ${arrayFrom1To5.mkString(" ")}") // note here: array to String will show obj reference
    println("--------------")

    // Pattern matching
    println("\nPattern matching by types")
    val priceOfDonut: Any = 2.50 // type infer to Double
    val priceType = priceOfDonut match {
      case price: Int => "Int"
      case price: Double => "Double"
      case price: Float => "Float"
      case price: String => "String"
      case price: Boolean => "Boolean"
      case price: Char => "Char"
      case price: Long => "Long"
    }
    println(s"Donut price type = $priceType")
    println("--------------")

    // create tuple, and tuple list pattern matching
    val chocolateDonut = ("Chocolate Donut", "Very Tasty", 3.0)
    val plainDonut = ("Plain Donut", "Tasty", 2.0)
    val glazedDonut = ("Glazed Donut", "Very Tasty", 3.5)
    val donutList = List(chocolateDonut, plainDonut, glazedDonut)
    donutList.foreach({
      case ("Plain Donut", taste, price) => println(s"Plain donut is $taste, and price is $price")
      case d if d._1 == "Glazed Donut" => println(s"Glazed donut is ${d._2}, and price is ${d._3}")
      case _ => None
    })
  }


}

