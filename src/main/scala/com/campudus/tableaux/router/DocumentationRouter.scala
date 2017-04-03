package com.campudus.tableaux.router

import java.net.URL

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.helper.DocUriParser
import io.vertx.ext.web.RoutingContext
import org.vertx.scala.router.routing._

import scala.io.Source
import scala.util.matching.Regex

object DocumentationRouter {

  def apply(config: TableauxConfig): DocumentationRouter = {
    new DocumentationRouter(config)
  }
}

class DocumentationRouter(override val config: TableauxConfig) extends BaseRouter {

  //val swaggerUiVersion = "3.0.3"
  val swaggerUiVersion = "2.2.10-1"

  val Index: Regex = "^/docs/index.html$".r
  val IndexRedirect: Regex = "^/docs$|^/docs/$".r

  val Swagger: Regex = "^/docs/swagger\\.json$".r

  val OtherFile: Regex = "^/docs/([A-Za-z0-9-_\\.]{1,60}){1}$".r
  val OtherFileWithDirectory: Regex = "^/docs/([A-Za-z0-9-_\\.]{1,60}){1}/([A-Za-z0-9-_\\.]{1,60}){1}$".r

  private def parseAbsoluteURI(absoluteURI: String): (String, String, String) = DocUriParser.parse(absoluteURI)

  override def routes(implicit context: RoutingContext): Routing = {
    case Get(IndexRedirect()) =>
      StatusCode(301, Header("Location", "/docs/index.html", NoBody))

    case Get(Index()) =>
      val uri = context.request().absoluteURI()
      val (scheme, host, basePath) = parseAbsoluteURI(uri)

      val is = getClass.getResourceAsStream(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/index.html")

      val swaggerURL = new URL(new URL(s"$scheme://$host"), basePath + "/docs/swagger.json")

      val file = Source.fromInputStream(is, "UTF-8")
        .mkString
        .replace(
          "http://petstore.swagger.io/v2/swagger.json",
          swaggerURL.toString
        )

      logger.info(s"Swagger $uri => ($scheme, $host, $basePath) => $swaggerURL")

      OkString(file, "text/html; charset=UTF-8")

    case Get(Swagger()) =>
      val uri = context.request().absoluteURI()
      val (scheme, host, basePath) = parseAbsoluteURI(uri)

      val is = getClass.getResourceAsStream(s"/swagger.json")
      val file = Source.fromInputStream(is, "UTF-8").mkString

      val json = file
        .replace("$SCHEME$", scheme)
        .replace("$HOST$", host)
        .replace("$BASEPATH$", basePath)

      OkString(json, "application/javascript; charset=UTF-8")

    case Get(OtherFile(file)) =>
      SendEmbeddedFile(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/$file")

    case Get(OtherFileWithDirectory(directory, file)) =>
      SendEmbeddedFile(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/$directory/$file")
  }
}
