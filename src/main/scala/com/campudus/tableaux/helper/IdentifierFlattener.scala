package com.campudus.tableaux.helper

import com.campudus.tableaux.database.GetReturn
import org.vertx.scala.core.json.JsonObject

object IdentifierFlattener {

  def isMultiLanguageValue(value: Any): Boolean =
    value match {
      case _: JsonObject => true
      case _ => false
    }

  /**
    * Returns true if at least one element of Seq is a MultiLanguageElement
    */
  def containsMultiLanguageValue(values: Seq[Any]): Boolean = values.exists(isMultiLanguageValue)

  /**
    *
    * @param maybeSeq of type Seq or
    * @return a flattened sequence
    */
  def flatten[A](maybeSeq: A): Seq[Any] = {
    maybeSeq match {
      case seq: Seq[_] => {
        seq flatten {
          case seq: Seq[_] => flatten(seq)
          case simpleValue => Seq(simpleValue)
        }
      }
      case simpleValue => Seq(simpleValue)
    }
  }

}

class IdentifierFlattener {}
