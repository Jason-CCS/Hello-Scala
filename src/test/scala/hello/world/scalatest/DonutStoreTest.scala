package hello.world.scalatest

import hello.world.scala.scala_test.DonutStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DonutStoreTest extends AnyFlatSpec with Matchers {
  behavior of "DonutStore class"

  // equity test
  "favorite donut" should "match vanilla donut" in {
    val donutStore = new DonutStore()
    donutStore.favoriteDonut() shouldEqual "vanilla donut"
    donutStore.favoriteDonut() === "vanilla donut"
    donutStore.favoriteDonut() should not equal "plain donut"
    donutStore.favoriteDonut() should not be "plain donut"
    donutStore.favoriteDonut() !== "plain donut"
  }

  // length test
  "Length and size of donuts" should "be equal to 3" in {
    val donutStore = new DonutStore()
    val donuts = donutStore.donuts()
    donuts should have size 3
    donuts should have length 3
  }

  //  boolean test
  "Example of boolean assertions" should "be valid" in {
    val donutStore = new DonutStore()
    val donuts = donutStore.donuts()
    donuts.nonEmpty shouldEqual true
    donuts.size === 3
    donuts.contains("chocolate donut") shouldEqual false
    donuts should not be empty
    donutStore.favoriteDonut() should not be empty
  }

  //collection test
  "Example of collection assertions" should "be valid" in {
    val donutStore = new DonutStore()
    val donuts = donutStore.donuts()
    donuts should contain("plain donut")
    donuts should not contain "chocolate donut"
    donuts shouldEqual Seq("vanilla donut", "plain donut", "glazed donut")
  }

  // type test
  "Examples of type assertions" should "be valid" in {
    val donutStore = new DonutStore()
    donutStore shouldBe a[DonutStore[_]]
    donutStore.favoriteDonut() shouldBe a[String]
    donutStore.donuts() shouldBe a[Seq[_]]
  }

  // exception test
  "Method DonutStore.printName()" should "throw IllegalStateException" in {
    val donutStore = new DonutStore()

    // assert msg of exception
    val exception = the [java.lang.IllegalStateException] thrownBy {
      donutStore.printName()
    }
    // here we verify that the exception class and the internal message
    exception.getClass shouldEqual classOf[java.lang.IllegalStateException]
    exception.getMessage should include ("Some Error")

    // only assert exception type
    an [java.lang.IllegalStateException] should be thrownBy {
      new DonutStore().printName()
    }
  }

  // private method test
  "Example of testing private method" should "be valid" in {
    val donutStore = new DonutStore()
    val priceWithDiscount = donutStore.donutPrice("vanilla donut")
    priceWithDiscount shouldEqual Some(1.6)

    // test the private method discountByDonut()
    import org.scalatest.PrivateMethodTester._
    val discountByDonutMethod = PrivateMethod[Double]('discountByDonut)
    val vanillaDonutDiscount = donutStore invokePrivate discountByDonutMethod("vanilla donut")
    vanillaDonutDiscount shouldEqual 0.2
  }
}
