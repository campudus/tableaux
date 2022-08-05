package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.helper.JsonUtils
import com.campudus.tableaux.router.auth.KeycloakAuthHandler

import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.lang.scala.json.Json
import io.vertx.scala.ext.web.RoutingContext

object TableauxUser {

  def apply(name: String, roles: Seq[String]): TableauxUser = {
    new TableauxUser(name, roles)
  }

  def apply(rc: RoutingContext): TableauxUser = {
    val tokenPayload = getTokenPayload(rc)
    val name = extractUserName(tokenPayload, rc)
    val roles = extractUserRoles(tokenPayload)

    new TableauxUser(name, roles)
  }

  private def extractUserRoles(tokenPayload: JsonObject): Seq[String] = {
    val roles: JsonArray =
      tokenPayload
        .getJsonObject("realm_access", Json.emptyObj())
        .getJsonArray("roles", Json.emptyArr())

    JsonUtils.asSeqOf[String](roles)
  }

  private def getCookieValue(defaultValue: String, name: String, rc: RoutingContext): String =
    rc.cookies().toSet
      .find(_.getName() == name)
      .map(_.getValue())
      .getOrElse(defaultValue)

  private def extractUserName(tokenPayload: JsonObject, rc: RoutingContext): String =
    Option(tokenPayload
      .getString("preferred_username"))
      .getOrElse(getCookieValue("unknown", "userName", rc))

  private def getTokenPayload(rc: RoutingContext): JsonObject = rc.get(KeycloakAuthHandler.TOKEN_PAYLOAD)

}

case class TableauxUser(name: String, roles: Seq[String])
