package com.campudus.tableaux.router

import java.net.URL

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.helper.DocUriParser
import io.vertx.scala.core.http.HttpServerRequest
import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.router.routing._

import scala.io.Source
import scala.util.matching.Regex

object DocumentationRouter {

  def apply(config: TableauxConfig): DocumentationRouter = {
    new DocumentationRouter(config)
  }
}

class DocumentationRouter(override val config: TableauxConfig) extends BaseRouter {

  val swaggerUiVersion = "3.17.6"

  val Index: Regex = "^/docs/index.html$".r
  val IndexRedirect: Regex = "^/docs$|^/docs/$".r

  val Swagger: Regex = "^/docs/swagger\\.json$".r

  val OtherFile: Regex = "^/docs/([A-Za-z0-9-_\\.]{1,60}){1}$".r
  val OtherFileWithDirectory: Regex = "^/docs/([A-Za-z0-9-_\\.]{1,60}){1}/([A-Za-z0-9-_\\.]{1,60}){1}$".r

  private def parseAbsoluteURI(request: HttpServerRequest): (String, String, String) = {

    /**
      * Sometimes we use tableaux behind some weird proxy configurations
      * If so we use x-forwarded headers to figure out how to point to swagger json
      */
    val forwardedScheme = request
      .getHeader("x-forwarded-proto")
      .flatMap(_.split(",").headOption)

    val forwardedHost = request
      .getHeader("x-forwarded-host")
      .flatMap(_.split(",").headOption)
      .map(forwardedHost => {
        forwardedHost.split(":").toList match {
          case List(host, "443") => host
          case List(host, "80") => host
          case _ => forwardedHost
        }
      })

    val forwardedUrl = request.getHeader("x-forwarded-url")

    val uri = (forwardedScheme, forwardedHost, forwardedUrl) match {
      case (Some(scheme), Some(host), Some(query)) => s"$scheme://$host$query"
      case _ => request.absoluteURI()
    }

    DocUriParser.parse(uri)
  }

//  override def routes(implicit context: RoutingContext): Routing = {
//    case Get(IndexRedirect()) =>
//      val (_, _, basePath) = parseAbsoluteURI(context.request())
//      val path = List(basePath, "docs", "index.html")
//        .flatMap({
//          case str if str.isEmpty => None
//          case str => Option(str)
//        })
//        .mkString("/")
//
//      StatusCode(301, Header("Location", s"/$path", NoBody))
//
//    case Get(Index()) =>
//      val (scheme, host, basePath) = parseAbsoluteURI(context.request())
//
//      val is = getClass.getResourceAsStream(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/index.html")
//
//      val swaggerURL = new URL(new URL(s"$scheme://$host"), basePath + "/docs/swagger.json")
//
//      val file = Source
//        .fromInputStream(is, "UTF-8")
//        .mkString
//        .replace(
//          "https://petstore.swagger.io/v2/swagger.json",
//          swaggerURL.toString
//        )
//
//      logger.info(s"Headers ${context.request().headers().asJava.asInstanceOf[io.vertx.core.MultiMap].entries()}")
//      logger.info(s"Swagger ${context.request().absoluteURI()} => ($scheme, $host, $basePath) => $swaggerURL")
//
//      OkString(file, "text/html; charset=UTF-8")
//
//    case Get(Swagger()) =>
//      val (scheme, host, basePath) = parseAbsoluteURI(context.request())
//
//      val is = getClass.getResourceAsStream(s"/swagger.json")
//      val file = Source.fromInputStream(is, "UTF-8").mkString
//
//      val json = file
//        .replace("$SCHEME$", scheme)
//        .replace("$HOST$", host)
//        .replace("$BASEPATH$", if (basePath.startsWith("/")) basePath else s"/$basePath")
//
//      OkString(json, "application/javascript; charset=UTF-8")
//
//    case Get(OtherFile(file)) =>
//      SendEmbeddedFile(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/$file")
//
//    case Get(OtherFileWithDirectory(directory, file)) =>
//      SendEmbeddedFile(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/$directory/$file")
//  }
}
