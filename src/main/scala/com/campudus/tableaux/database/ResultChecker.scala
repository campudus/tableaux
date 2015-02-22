package com.campudus.tableaux.database

import org.vertx.scala.core.json.JsonObject
import org.vertx.scala.core.json.JsonArray
import com.campudus.tableaux.NotFoundInDatabaseException

object ResultChecker {

  def getJsonArray(json: JsonObject): JsonArray = json.getArray("results")

  def deleteNotNull(json: JsonObject): JsonArray = checkHelper(json, "DELETE 0", "delete")

  def selectNotNull(json: JsonObject): JsonArray = checkHelper(json, "SELECT 0", "select")

  def insertNotNull(json: JsonObject): JsonArray = checkHelper(json, "INSERT 0", "insert")

  private def checkHelper(json: JsonObject, s: String, ex: String): JsonArray = {
    if (json.getString("message") == s) throw NotFoundInDatabaseException(s"Warning: $ex query failed", ex) else json.getArray("results")
  }
}