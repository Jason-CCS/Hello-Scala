name := "Hello-Scala"
version := "0.1"
scalaVersion := "2.12.18"

// https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging
libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "io.delta" %% "delta-spark" % "3.2.0",

  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.scalactic" %% "scalactic" % "3.2.9" % Test
)


