package com.campudus.tableaux.helper

import org.vertx.scala.core.json.JsonObject

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
    *
    * @param maybeSeq of type Seq or
    * @return a flattened sequence
    */
  private[helper] def flatSeq[A](maybeSeq: A): Seq[Any] = {
    maybeSeq match {
      case seq: Seq[_] => {
        seq flatten {
          case seq: Seq[_] => flatSeq(seq)
          case simpleValue => Seq(simpleValue)
        }
      }
      case simpleValue => Seq(simpleValue)
    }
  }

  private[helper] def flattenToJsonObject[A](maybeSeq: A): Seq[Any] = {
    maybeSeq match {
      case seq: Seq[_] => {
        seq flatten {
          case seq: Seq[_] => flatSeq(seq)
          case simpleValue => Seq(simpleValue)
        }
      }
      case simpleValue => Seq(simpleValue)
    }
  }

  private[helper] def concatenate(values: Seq[Any], sep: String = " "): String = {
    values.map(_.toString).mkString(sep)
  }

  def flatten[A](maybeSeq: A): String = {
//    TODO case für return string und JsonObject (containsMultiLanguageValue)
    concatenate(flatSeq(maybeSeq))
  }

}

class IdentifierFlattener {}
