package com.campudus.tableaux.helper

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
  private[helper] def containsMultiLanguageValue(values: Seq[Any]): Boolean = values.exists(isMultiLanguageValue)

  /**
    * @param maybeSeq
    *   any type or seq of mixed types (also deeply nested) that is stored in cell value
    * @return
    *   a flattened sequence
    */
  private[helper] def flatten[A](maybeSeq: A): Seq[Any] = {
    def flattenSeq[A](innerMaybeSeq: A): Seq[Any] = {
      innerMaybeSeq match {
        case Some(s) => flattenSeq(s)
        case None | Nil => Seq.empty[A]
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

    maybeSeq match {
      case Some(s) => flatten(s)
      case None | Nil => Seq.empty[A]
      case seq: Seq[_] => flattenSeq(seq)
      case ja: JsonArray => flattenSeq(ja)
      case simpleValue => Seq(simpleValue)
    }
  }

  private[helper] def concatenateSingleLang(values: Seq[Any], sep: String = " "): String = {
    values.filter(_ != null).map(_.toString.trim).filter(!_.isEmpty).mkString(sep)
  }

  private[helper] def concatenateMultiLang(langtags: Seq[String], flatValues: Seq[Any]): JsonObject = {

    val defaultLang = langtags.headOption.getOrElse("")

    val jsonTuples = langtags.map(langtag => {
      val valueList = flatValues.map({
        case languageObject: JsonObject => getLanguageValue(languageObject, langtag, defaultLang)
        case v => v
      })
      (langtag, concatenateSingleLang(valueList))
    })

    Json.obj(jsonTuples: _*)
  }

  /**
    * Get string for a specified langtag If langtag key doesn't exist, return string for fallbackLangtag else empty
    * string
    */
  private def getLanguageValue(languageObject: JsonObject, langtag: String, fallbackLangtag: String): String = {
    val map = languageObject.getMap
    val fallbackValue = map.getOrDefault(fallbackLangtag, "")
    Option(map.getOrDefault(langtag, fallbackValue)) match {
      case Some(v) => v.toString
      case None => ""
    }
  }

  /**
    * Compresses a maybe nested sequence of single- and/or multilanguage values into a single object. There are two
    * cases: (1) If the sequence contains at least one multilanguage item the result is a Multilanguage item
    * (JsonObject) (2) else result type is a Singlelanguage item (String)
    */
  def compress[A](langtags: Seq[String], maybeSeq: A): Either[String, JsonObject] = {
    val flatSeq: Seq[Any] = flatten(Option(maybeSeq))

    if (containsMultiLanguageValue(flatSeq))
      Right(concatenateMultiLang(langtags, flatSeq))
    else
      Left(concatenateSingleLang(flatSeq))
  }
}
