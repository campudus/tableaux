package com.campudus.tableaux.helper

import com.campudus.tableaux.NotFoundInDatabaseException
import org.vertx.scala.core.json.{JsonArray, JsonObject}

/**
 * Checks if database result changed
 * something or not.
 */
object ResultChecker {

  def getSeqOfJsonArray(json: JsonObject): Seq[JsonArray] = {
    jsonArrayToSeq(json.getArray("results"))
  }

  def jsonArrayToSeq[A](json: JsonArray): Seq[A] = {
    import scala.collection.JavaConverters._
    json.asScala.toSeq.asInstanceOf[Seq[A]]
  }

  def deleteNotNull(json: JsonObject): Seq[JsonArray] = checkHelper(json, "DELETE 0", "delete")

  def selectNotNull(json: JsonObject): Seq[JsonArray] = checkHelper(json, "SELECT 0", "select")

  def insertNotNull(json: JsonObject): Seq[JsonArray] = checkHelper(json, "INSERT 0", "insert")

  def updateNotNull(json: JsonObject): Seq[JsonArray] = checkHelper(json, "UPDATE 0", "update")

  private def checkHelper(json: JsonObject, message: String, queryType: String): Seq[JsonArray] = {
    if (json.getString("message") == message) {
      throw NotFoundInDatabaseException(s"Warning: $message query failed", queryType)
    } else {
      getSeqOfJsonArray(json)
    }
  }
}
