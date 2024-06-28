package hello.world.scala.di

trait DonutDatabase[A] {
  def addOrUpdate(donut: A): Long

  def query(donut: A): A

  def delete(donut: A): Boolean
}
