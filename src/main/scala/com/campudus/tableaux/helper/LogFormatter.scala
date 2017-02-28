package com.campudus.tableaux.helper

import java.io.{PrintWriter, StringWriter}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.logging.{Formatter, LogRecord}

object LogFormatter {

  val dateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
}

/**
  * Created by alexandervetter on 23/02/17.
  */
class LogFormatter extends Formatter {

  import LogFormatter._

  override def format(record: LogRecord): String = {

    val dateTime = Instant.ofEpochMilli(record.getMillis).atZone(ZoneId.systemDefault()).toLocalDateTime

    val builder = new StringBuilder(1000)

    builder.append(dateFormat.format(dateTime)).append(" ")
    builder.append(String.format("%1$-7s", record.getLevel)).append(" ")

    builder.append("[").append(String.format("%1$-30s", shortenClassName(record.getLoggerName))).append("] ")

    builder.append(formatMessage(record))

    Option(record.getThrown)
      .foreach(cause => {
        val sw = new StringWriter()
        cause.printStackTrace(new PrintWriter(sw))
        val exceptionAsString = sw.toString

        builder.append("\n").append(exceptionAsString)
      })

    builder.append("\n")

    builder.toString()
  }

  private def shortenClassName(className: String): String = {
    val splitted = Option(className)
      .getOrElse("")
      .split("\\.")
      .toList

    splitted.zipWithIndex
      .map({
        case (part, index) =>
          if (index + 1 >= splitted.length) {
            part
          } else {
            part.substring(0, 1)
          }
      })
      .mkString(".")
  }
}
