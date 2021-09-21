name := "Hello-Scala"
version := "0.1"
scalaVersion := "2.12.8"

// https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging
libraryDependencies ++= Seq("com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)


