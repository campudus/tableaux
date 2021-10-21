package com.campudus.tableaux

import com.campudus.tableaux.helper.JsonUtils
import io.vertx.core.json.JsonArray
import org.vertx.scala.core.json.{Json, JsonObject}
import io.vertx.scala.ext.web.Cookie

object RequestContext {
  def apply(): RequestContext = new RequestContext()
}

class RequestContext() {

  // field has to be mutable and is only set from TableauxAuthHandler::handle
  var principal: JsonObject = Json.emptyObj() // scalastyle:ignore

  // for testing purposes, unit tests must reset the principal in setUp method
  def resetPrincipal(): Unit = principal = Json.emptyObj()

  def getPrincipleString(name: String): String = {
    principal.getString(name)
  }

  def getUserRoles: Seq[String] = {
    val roles: JsonArray =
      principal
        .getJsonObject("realm_access", Json.emptyObj())
        .getJsonArray("roles", Json.emptyArr())

    JsonUtils.asSeqOf[String](roles)
  }

// field has to be mutable and is only set from RouterRegistry::routes
  var cookies: Set[Cookie] = Set.empty[Cookie] // scalastyle:ignore

  def getCookieValue(name: String, defaultValue: String = "dev"): String = {
    cookies.find(_.getName() == name).map(_.getValue()).getOrElse(defaultValue)
  }
}
