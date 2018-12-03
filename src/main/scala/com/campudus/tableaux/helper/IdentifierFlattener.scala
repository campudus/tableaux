package com.campudus.tableaux.helper

import com.campudus.tableaux.helper.IdentifierFlattener.flattenSeq
import io.vertx.core.json.{JsonArray, JsonObject}
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

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
      case None | Nil | null => Seq.empty
      case seq: Seq[_] => {
        seq flatten {
          case seq: Seq[_] => flattenSeq(seq)
          case ja: JsonArray => flattenSeq(ja)
          case o: JsonObject => Seq(o)
          case v => Seq(v)
        }
      }
      case ja: JsonArray => {
        ja.asScala flatten {
          case seq: Seq[_] => flattenSeq(seq)
          case ja: JsonArray => flattenSeq(ja)
          case o: JsonObject => Seq(o)
          case v => Seq(v)
        }
      }.toSeq
      case simpleValue => Seq(simpleValue)
    }
  }

  private[helper] def flatten[A](maybeSeq: A): Seq[Any] = {
    maybeSeq match {
      case Some(s) => flatten(s)
      case None | Nil | null => Seq.empty
      case seq: Seq[_] => flattenSeq(seq)
      case ja: JsonArray => flattenSeq(ja)
      case simpleValue => Seq(simpleValue)
    }
  }

  private[helper] def concatenateSingleLang(values: Seq[Any], sep: String = " "): String = {
    values.filter(_ != null).map(_.toString.trim).filter(!_.isEmpty).mkString(sep)
  }

  private[helper] def concatenateMultiLang(langtags: Seq[String], flatValues: Seq[Any]): JsonObject = {

    val defaultLang = langtags.head

    val jsonTuples = langtags.map(langtag => {
      val valueList = flatValues.map({
        case languageObject: JsonObject => getLanguageValue(languageObject, langtag, defaultLang)
        case v => v
      })
      (langtag, concatenateSingleLang(valueList))
    })

    Json.obj(jsonTuples: _*)
  }

  private def getLanguageValue(languageObject: JsonObject, langtag: String, fallbackLangtag: String) = {
    val map = languageObject.getMap
    val fallbackValue = map.getOrDefault(fallbackLangtag, "")
    map.getOrDefault(langtag, fallbackValue)
  }

  def compress[A](langtags: Seq[String], maybeSeq: A): Either[String, JsonObject] = {
    val flatSeq: Seq[Any] = flattenSeq(Option(maybeSeq))

    if (containsMultiLanguageValue(flatSeq))
      Right(concatenateMultiLang(langtags, flatSeq))
    else
      Left(concatenateSingleLang(flatSeq))
  }
}

class IdentifierFlattener {}
