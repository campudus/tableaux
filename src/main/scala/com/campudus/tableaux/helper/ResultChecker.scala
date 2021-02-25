package com.campudus.tableaux.helper

import com.campudus.tableaux.{DatabaseException, NotFoundInDatabaseException}
import org.vertx.scala.core.json.{JsonArray, JsonObject}

/**
  * Checks if database result changed
  * something or not.
  */
object ResultChecker {

  def resultObjectToJsonArray(json: JsonObject): Seq[JsonArray] = {
    jsonArrayToSeq(json.getJsonArray("results")).map(_.asInstanceOf[JsonArray])
  }

  def jsonArrayToSeq(json: JsonArray): Seq[Any] = {
    import scala.collection.JavaConverters._
    Option(json).map(_.asScala.toSeq.asInstanceOf[Seq[Any]]).getOrElse(Seq.empty)
  }

  def deleteNotNull(json: JsonObject): Seq[JsonArray] = checkNotNull(json, "delete")

  def selectNotNull(json: JsonObject): Seq[JsonArray] = checkNotNull(json, "select")

  def insertNotNull(json: JsonObject): Seq[JsonArray] = checkNotNull(json, "insert")

  def updateNotNull(json: JsonObject): Seq[JsonArray] = checkNotNull(json, "update")

  private def checkNotNull(json: JsonObject, queryType: String): Seq[JsonArray] = {
    val message = queryType.toUpperCase.concat(" 0")

    if (json.getString("message") == message) {
      throw NotFoundInDatabaseException(s"Warning: $message query failed", queryType)
    } else {
      resultObjectToJsonArray(json)
    }
  }

  def deleteCheckSize(json: JsonObject, size: Int): Seq[JsonArray] = checkSize(json, "delete", size)

  def selectCheckSize(json: JsonObject, size: Int): Seq[JsonArray] = checkSize(json, "select", size)

  def insertCheckSize(json: JsonObject, size: Int): Seq[JsonArray] = checkSize(json, "insert", size)

  private def checkSize(json: JsonObject, queryType: String, size: Int): Seq[JsonArray] = {
    println(json, size)
    if (json.getInteger("rows") != size) {
      throw DatabaseException(
        s"Error: query failed because result size (${json.getInteger("rows")}) doesn't match expected size ($size)",
        "checkSize")
    } else {
      resultObjectToJsonArray(json)
    }
  }
}
