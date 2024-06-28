package hello.world.scala.function

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * callback function 就是 return is void
 */
object CallbackFunc extends App {
  def printReportWithOptionCallback(sendEmailCallback: Option[String => Unit]) {
    val dateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYY/MM/dd HH:mm:ss"))
    val report = s"[$dateTime] This is your report."
    println(report)
    sendEmailCallback.foreach(f => f(dateTime))
  }

  def sendEmailFunc(dateTime: String): Unit = {
    println(s"current datetime is $dateTime")
    println("sending email...")
    // no return here
  }

  printReportWithOptionCallback(None)
  println("=====")
  printReportWithOptionCallback(Some(sendEmailFunc))
}
