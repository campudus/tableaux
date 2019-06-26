package com.campudus.tableaux

import com.campudus.tableaux.helper.JsonUtils
import io.vertx.core.json.JsonArray
import org.vertx.scala.core.json.{Json, JsonObject}

object RequestContext {
  def apply(): RequestContext = new RequestContext()
}

class RequestContext() {

  // field has to be mutable and is only set from TableauxAuthHandler::handle
  var principal: JsonObject = Json.emptyObj() // scalastyle:ignore

  // for testing purposes, unit tests must reset the principal in setUp method
  def resetPrincipal = principal = Json.emptyObj()

  def getPrincipleString(name: String, defaultValue: String): String = {
    principal.getString(name, defaultValue)
  }

  def getUserRoles: Seq[String] = {
    // println(s"XXXXXX principal: $principal")
    val roles: JsonArray =
      principal
        .getJsonObject("realm_access", Json.emptyObj())
        .getJsonArray("roles", Json.emptyArr())

    JsonUtils.asSeqOf[String](roles)
  }
}
