package com.campudus.tableaux.testtools

import com.campudus.tableaux.helper.VertxAccess

import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.auth.jwt.{JWTAuth, JWTAuthOptions, JWTOptions}
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.collection.JavaConverters._

object JsonTestHelper {

  def getListFromKey[T](response: JsonObject, key: String): List[T] = {
    response.getJsonArray(key).asScala
      .map(_.asInstanceOf[T]).toList
  }

  def getRows[T](response: JsonObject): List[T] = getListFromKey[T](response, "rows")

  def getHistoryRows(response: JsonObject): List[JsonObject] = getRows[JsonObject](response)
}
