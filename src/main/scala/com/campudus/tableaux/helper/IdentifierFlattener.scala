package com.campudus.tableaux.helper

import io.vertx.core.json.JsonArray
import org.vertx.scala.core.json.{JsonArray, JsonObject}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object IdentifierFlattener {

  private[helper] def isMultiLanguageValue(value: Any): Boolean =
    value match {
      case _: JsonObject => true
      case _ => false
    }

  /**
    * Returns true if at least one element of Seq is a MultiLanguageElement
    */
  private[helper] def containsMultiLanguageValue[A](values: A): Boolean =
    values match {
      case s: Seq[_] => s.exists(isMultiLanguageValue)
      case v => isMultiLanguageValue(v)
    }

  /**
    *
    * @param maybeSeq of type Seq or
    * @return a flattened sequence
    */
  private def flattenSeq[A](maybeSeq: A): Seq[Any] = {
    maybeSeq match {
      case Some(s) => flattenSeq(s)
      case None => Seq.empty
      case seq: Seq[_] => {
        seq flatten {
          case seq: Seq[_] => flattenSeq(seq)
          case ja: JsonArray => flattenSeq(ja)
          case simpleValue => Seq(simpleValue)
        }
      }
      case ja: JsonArray => {
        ja.asScala flatten {
          case seq: Seq[_] => flattenSeq(seq)
          case ja: JsonArray => flattenSeq(ja)
          case o: JsonObject => Seq(o)
        }
      }.toSeq
      case simpleValue => Seq(simpleValue)
    }
  }

  private[helper] def flatten[A](maybeSeq: A): Seq[Any] = {
    maybeSeq match {
      case Some(s) => flatten(s)
      case None => Seq.empty
      case seq: Seq[_] => flattenSeq(seq)
      case ja: JsonArray => flattenSeq(ja)
      case simpleValue => Seq(simpleValue)
    }
  }

  private[helper] def concatenateSingleLang(values: Seq[Any], sep: String = " "): String = {
    values.map(_.toString).mkString(sep)
  }

  private[helper] def concatenateMultiLang(values: Seq[Any], sep: String = " "): String = {
    values.foreach(a => println("XXX: " + a))

    values.map(_.toString).mkString(sep)
  }

  def compress[A](maybeSeq: A): String = {
//    TODO case für return string und JsonObject (containsMultiLanguageValue)
    println("XXX is es a multi? " + containsMultiLanguageValue(maybeSeq))
//    if (containsMultiLanguageValue(maybeSeq))
//      concatenateMultiLang(flatJsonObjectSeq(maybeSeq))
//    else
    concatenateSingleLang(flattenSeq(Option(maybeSeq)))
  }
}

class IdentifierFlattener {}
