package com.campudus.tableaux.router

import java.net.URL

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.helper._
import io.vertx.scala.core.http.HttpServerRequest
import io.vertx.scala.ext.web.{Router, RoutingContext}

import scala.io.Source

object DocumentationRouter {

  def apply(config: TableauxConfig): DocumentationRouter = {
    new DocumentationRouter(config)
  }
}

class DocumentationRouter(override val config: TableauxConfig) extends BaseRouter {

  val swaggerUiVersion = "3.17.6"

  val Index: String = "/index.html"
  val Swagger: String = "/swagger.json"

  val File: String = s"""/(?<file>[A-Za-z0-9-_\\.]{1,60}){1}"""
  val FileWithDirectory: String = s"""/(?<directory>[A-Za-z0-9-_\\.]{1,60}){1}/(?<file>[A-Za-z0-9-_\\.]{1,60}){1}"""

  def route: Router = {
    val router = Router.router(vertx)

    router.get(Index).handler(index)
    router.get("/").handler(indexRedirect)
    router.get(Swagger).handler(getSwagger)
    router.getWithRegex(File).handler(getFile)
    router.getWithRegex(FileWithDirectory).handler(getFileWithDirectory)

    router
  }

  private def index(context: RoutingContext): Unit = {
    val (scheme, host, basePath) = parseAbsoluteURI(context.request())

    val is = getClass.getResourceAsStream(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/index.html")

    val swaggerURL = new URL(new URL(s"$scheme://$host"), basePath + "/docs/swagger.json")

    val file = Source
      .fromInputStream(is, "UTF-8")
      .mkString
      .replace("https://petstore.swagger.io/v2/swagger.json", swaggerURL.toString)

    logger.info(s"Headers ${context.request().headers().asJava.asInstanceOf[io.vertx.core.MultiMap].entries()}")
    logger.info(s"Swagger ${context.request().absoluteURI()} => ($scheme, $host, $basePath) => $swaggerURL")

    sendReply(context, OkString(file, "text/html; charset=UTF-8"))
  }

  private def indexRedirect(context: RoutingContext): Unit = {
    val (_, _, basePath) = parseAbsoluteURI(context.request())
    val path = List(basePath, "docs", "index.html")
      .flatMap({
        case str if str.isEmpty => None
        case str => Option(str)
      })
      .mkString("/")

    sendReply(context, StatusCode(301, Header("Location", s"/$path", NoBody)))
  }

  private def getSwagger(context: RoutingContext): Unit = {
    val (scheme, host, basePath) = parseAbsoluteURI(context.request())

    val is = getClass.getResourceAsStream(s"/swagger.json")
    val file = Source.fromInputStream(is, "UTF-8").mkString

    val json = file
      .replace("$SCHEME$", scheme)
      .replace("$HOST$", host)
      .replace("$BASEPATH$", if (basePath.startsWith("/")) basePath else s"/$basePath")

    sendReply(context, OkString(json, "application/javascript; charset=UTF-8"))
  }

  private def getFile(context: RoutingContext): Unit = {
    for {
      file <- getStringParam("file", context)
    } yield sendReply(context, SendEmbeddedFile(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/$file"))
  }
  private def getFileWithDirectory(context: RoutingContext): Unit = {
    for {
      directory <- getStringParam("directory", context)
      file <- getStringParam("file", context)
    } yield
      sendReply(context, SendEmbeddedFile(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/$directory/$file"))
  }

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
}
