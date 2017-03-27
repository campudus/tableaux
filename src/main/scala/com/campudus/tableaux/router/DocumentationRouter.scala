package com.campudus.tableaux.router

import com.campudus.tableaux.TableauxConfig
import io.vertx.ext.web.RoutingContext
import org.vertx.scala.router.routing.{Get, SendEmbeddedFile}

import scala.util.matching.Regex

object DocumentationRouter {

  def apply(config: TableauxConfig): DocumentationRouter = {
    new DocumentationRouter(config)
  }
}

class DocumentationRouter(override val config: TableauxConfig) extends BaseRouter {

  val swaggerUiVersion = "3.0.2"

  val Index: Regex = "^/docs$|^/docs/$|^/docs/index.html$".r
  val Swagger: Regex = "^/docs/swagger\\.json$".r
  val OtherFile: Regex = "^/docs/([A-Za-z0-9-_\\.]*)$".r
  val OtherFileWithDirectory: Regex = "^/docs/([A-Za-z0-9-_\\.]*)/([A-Za-z0-9-_\\.]*)$".r

  override def routes(implicit context: RoutingContext): Routing = {
    case Get(Index()) =>
      // use custom swagger-ui html
      SendEmbeddedFile("/swagger-ui.html")

    case Get(Swagger()) =>
      SendEmbeddedFile("/swagger.json")

    case Get(OtherFile(file)) =>
      SendEmbeddedFile(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/$file")

    case Get(OtherFileWithDirectory(directory, file)) =>
      SendEmbeddedFile(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/$directory/$file")
  }
}
