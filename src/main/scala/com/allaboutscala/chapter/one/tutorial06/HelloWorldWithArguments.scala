package com.allaboutscala.chapter.one.tutorial06

object HelloWorldWithArguments extends App {
  println("Hello world with arguments scala application!")

  println("Command line args are: ")
  println(args.mkString(", "))
}
