package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.helper.JsonUtils
import com.campudus.tableaux.router.auth.KeycloakAuthHandler

import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.lang.scala.json.Json
import io.vertx.scala.ext.web.RoutingContext

import com.typesafe.scalalogging.LazyLogging

object TableauxUser extends LazyLogging {

  def apply(name: String, roles: Seq[String]): TableauxUser = {
    new TableauxUser(name, roles)
  }

  def apply(rc: RoutingContext): TableauxUser = {
    val tokenPayloadOpt = getTokenPayload(rc)
    val name = extractUserName(tokenPayloadOpt, rc)
    val roles = extractUserRoles(tokenPayloadOpt)

    logger.debug(s"Create TableauxUser '$name' with roles '$roles'")
    new TableauxUser(name, roles)
  }

  private def extractUserRoles(tokenPayloadOpt: Option[JsonObject]): Seq[String] = {
    val roles: JsonArray =
      tokenPayloadOpt match {
        case Some(tokenPayload) =>
          tokenPayload.getJsonObject("realm_access", Json.emptyObj()).getJsonArray("roles", Json.emptyArr())
        case None => Json.emptyArr()
      }
    JsonUtils.asSeqOf[String](roles)
  }

  private def getCookieValue(defaultValue: String, name: String, rc: RoutingContext): String =
    rc.cookies().toSet
      .find(_.getName() == name)
      .map(_.getValue())
      .getOrElse(defaultValue)

  private def extractUserName(tokenPayloadOpt: Option[JsonObject], rc: RoutingContext): String =
    tokenPayloadOpt match {
      case Some(tokenPayload) =>
        tokenPayload.getString("preferred_username", "unknown")
      case None => getCookieValue("unknown", "userName", rc)
    }

  private def getTokenPayload(rc: RoutingContext): Option[JsonObject] =
    Option(rc.get(KeycloakAuthHandler.TOKEN_PAYLOAD))
}

case class TableauxUser(name: String, roles: Seq[String])
