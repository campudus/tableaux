package com.campudus.tableaux

import org.vertx.scala.core.json.{ JsonObject, JsonArray }
import scala.collection.JavaConverters._
import scala.util.Try

sealed trait Matcher {
  def matchTo[T](name: String, value: T): Any
}

case object NumberMatcher extends Matcher {
  def matchTo[T](name: String, value: T): Long = value match {
    case v: Number => v.longValue()
    case other => throw InvalidJsonException(s"Warning: $name should be numbers, but got ${other.getClass}", "invalid")
  }
}

case object StringMatcher extends Matcher {
  def matchTo[T](name: String, value: T): String = value match {
    case v: String => v
    case other => throw InvalidJsonException(s"Warning: $name Should be string, but got ${other.getClass}", "invalid")
  }
}

case object AnyMatcher extends Matcher {
  def matchTo[T](name: String, value: T): Any = value match {
    case v: Number => v.longValue()
    case v: String => v
    case other => throw InvalidJsonException(s"Warning: $name should not be ${other.getClass}", "invalid")
  }
}

sealed trait Converter {
  def convert[T](name: String, matcher: => Matcher, value: T): Any
}

case object RowConverter extends Converter {
  def convert[T](name: String, matcher: => Matcher, value: T): Seq[_] = {
    val a = Try(Option(value.asInstanceOf[JsonArray]).get).getOrElse(throw InvalidJsonException(s"Warning: $name are not in a JSON array", "array")).asScala.toSeq
    if (a.isEmpty) throw InvalidJsonException(s"Warning: $name is empty", "empty")
    a map { matcher.matchTo(name, _) }
  }
}

case object ColumnConverter extends Converter {
  def convert[T](name: String, matcher: => Matcher, value: T): Any = {
    matcher.matchTo(name, value)
  }
}

object HelperFunctions {

  def jsonToSeqOfColumnNameAndType(json: JsonObject): Seq[(String, String)] = {
    jsonToSeq(json, "columnName", "type", StringMatcher, StringMatcher, ColumnConverter).asInstanceOf[Seq[(String, String)]]
  }

  def jsonToSeqOfRowsWithColumnIdAndValue(json: JsonObject): Seq[Seq[(Long, _)]] = {
    jsonToSeq(json, "columnIds", "values", NumberMatcher, AnyMatcher, RowConverter).asInstanceOf[Seq[Seq[(Long, _)]]]
  }

  private def jsonToSeq(json: JsonObject, firstName: String, secondName: String, firstMatcher: => Matcher, secondMatcher: => Matcher, converter: => Converter): Seq[_] = {
    val firstSeq = jsonToSeqHelper(json, firstName, firstMatcher, converter)
    val secondSeq = jsonToSeqHelper(json, secondName, secondMatcher, converter)
    val zippedSeq = firstSeq.zip(secondSeq)

    matchSizes(firstSeq, secondSeq, firstName, secondName)

    zippedSeq map {
      case (f: Seq[_], s: Seq[_]) =>
        matchSizes(f, s, firstName, secondName)
        f.zip(s)
      case s => s
    }
  }

  private def jsonToSeqHelper(json: JsonObject, name: String, matcher: => Matcher, converter: => Converter): Seq[_] = {
    val jsonArray = Try(Option(json.getArray(name)).get).getOrElse(throw InvalidJsonException(s"Warning: $name is null", "null")).asScala.toSeq
    if (jsonArray.isEmpty) throw InvalidJsonException(s"Warning: $name is empty", "empty")
    jsonArray.map(converter.convert(name, matcher, _))
  }

  private def matchSizes(firstSeq: Seq[_], secondSeq: Seq[_], firstName: String, secondName: String): Unit = {
    if (firstSeq.size != secondSeq.size) throw NotEnoughArgumentsException(s"Warning: $firstName and $secondName size doesn't match", "arguments")
  }
}