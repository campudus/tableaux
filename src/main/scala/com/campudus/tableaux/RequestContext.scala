package com.campudus.tableaux
import io.vertx.scala.ext.web.Cookie

object RequestContext {
  def apply(): RequestContext = new RequestContext()
}

class RequestContext() {

  var cookies: Set[Cookie] = Set.empty[Cookie]

  def getCookieValue(name: String, defaultValue: String = "dev"): String = {
    cookies.find(_.getName() == name).map(_.getValue()).getOrElse(defaultValue)
  }
}
