package com.campudus.tableaux

import org.vertx.scala.core.json.{Json, JsonObject}

object RequestContext {
  def apply(): RequestContext = new RequestContext()
}

class RequestContext() {

  // field has to be mutable and is only set from TableauxAuthHandler::handle
  var principal: JsonObject = Json.emptyObj() // scalastyle:ignore

  def getPrincipleString(name: String, defaultValue: String): String = {
    principal.getString(name, defaultValue)
  }
}
