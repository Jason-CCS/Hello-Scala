package hello.world.scala.di

trait DonutShoppingCartService[A] extends DonutShoppingCartDao[A] with DonutInventoryService[A]{
  override val donutDatabase: DonutDatabase[A] = new CassandraDonutStore[A]()
}
