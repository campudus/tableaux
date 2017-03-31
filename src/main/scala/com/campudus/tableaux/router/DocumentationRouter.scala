package com.campudus.tableaux.router

import java.net.URL

import com.campudus.tableaux.TableauxConfig
import io.vertx.ext.web.RoutingContext
import org.vertx.scala.router.routing.{Get, OkString, SendEmbeddedFile}

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
  val Swagger: Regex = "^/docs/swagger\\.json$".r
  val OtherFile: Regex = "^/docs/([A-Za-z0-9-_\\.]*)$".r
  val OtherFileWithDirectory: Regex = "^/docs/([A-Za-z0-9-_\\.]*)/([A-Za-z0-9-_\\.]*)$".r

  private def parseAbsoluteURI(absoluteURI: String): (String, String, String) = {
    "(https?)://(.*)/docs.*".r.unapplySeq(absoluteURI) match {
      case Some(List(scheme, apiPath)) =>
        apiPath.split("/").toList match {
          case host :: rest =>
            (scheme, host, rest.mkString("/"))
          case _ =>
            (scheme, "localhost:8181", "")
        }
      case _ =>
        ("http", "localhost:8181", "")
    }
  }

  override def routes(implicit context: RoutingContext): Routing = {
    case Get(Index()) =>
      val uri = context.request().absoluteURI()
      val (scheme, host, basePath) = parseAbsoluteURI(uri)

      val is = getClass.getResourceAsStream(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/index.html")

      val swaggerURL = new URL(new URL(new URL(s"$scheme://$host"), basePath), "/docs/swagger.json")

      val file = Source.fromInputStream(is, "UTF-8")
        .mkString
        .replace(
          "http://petstore.swagger.io/v2/swagger.json",
          swaggerURL.toString
        )

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
