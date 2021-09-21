package com.allaboutscala.chapter.one.tutorial07

object HelloWorldWithArgumentsDebug extends App {
  println("Hello world with arguments scala application!")

  println("Command line args are: ")
  println(args.mkString(", ")) // debug break point here
}
