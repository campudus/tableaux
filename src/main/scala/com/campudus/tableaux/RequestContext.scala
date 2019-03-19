package com.campudus.tableaux
import java.util.concurrent.atomic.AtomicReference

import io.vertx.scala.ext.web.Cookie

object RequestContext {
  def apply(): RequestContext = new RequestContext()
}

class RequestContext() {

  // field has to be mutable and is only set from RouterRegistry::routes
  var cookies: Set[Cookie] = Set.empty[Cookie] // scalastyle:ignore

  def getCookieValue(name: String, defaultValue: String = "dev"): String = {
    cookies.find(_.getName() == name).map(_.getValue()).getOrElse(defaultValue)
  }
}
