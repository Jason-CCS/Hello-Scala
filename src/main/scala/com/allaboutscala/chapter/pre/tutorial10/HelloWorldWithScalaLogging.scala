package com.allaboutscala.chapter.pre.tutorial10

import com.typesafe.scalalogging.LazyLogging

object HelloWorldWithScalaLogging extends App with LazyLogging{
  logger.info("Hello world from scala logging.")
}
