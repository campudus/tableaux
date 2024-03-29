package com.campudus.tableaux.router

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.helper._

import io.vertx.scala.core.http.HttpServerRequest
import io.vertx.scala.ext.web.{Router, RoutingContext}

import scala.io.Source

import java.net.URL

object DocumentationRouter {

  def apply(config: TableauxConfig): DocumentationRouter = {
    new DocumentationRouter(config)
  }
}

class DocumentationRouter(override val config: TableauxConfig) extends BaseRouter {

  private val swaggerUiVersion = "5.9.0"
  private val directory = """(?<directory>[A-Za-z0-9-_\\.]{1,60}){1}"""
  private val file = s"""(?<file>[A-Za-z0-9-_\\.]{1,60}){1}"""

  def route: Router = {
    val router = Router.router(vertx)

    router.get("/index.html").handler(index)
    router.get("/").handler(indexRedirect)
    router.get("/swagger-initializer.js").handler(retrieveInitializer)
    router.get("/swagger.json").handler(retrieveSwagger)
    router.getWithRegex(s"/$file").handler(retrieveFile)
    router.getWithRegex(s"/$directory/$file").handler(retrieveFileWithDirectory)

    router
  }

  private def index(context: RoutingContext): Unit = {
    val (scheme, host, basePath) = parseAbsoluteURI(context.request())

    logger.info(s"Headers ${context.request().headers().asJava.asInstanceOf[io.vertx.core.MultiMap].entries()}")

    sendReply(
      context,
      SendEmbeddedFile(
        s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/index.html"
      )
    )
  }

  private def retrieveInitializer(context: RoutingContext): Unit = {
    val (scheme, host, basePath) = parseAbsoluteURI(context.request())

    val inputStream =
      getClass.getResourceAsStream(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/swagger-initializer.js")

    val swaggerURL = new URL(new URL(s"$scheme://$host"), basePath + "/docs/swagger.json")
    logger.info(s"Swagger ${context.request().absoluteURI()} => ($scheme, $host, $basePath) => $swaggerURL")

    val file = Source
      .fromInputStream(inputStream, "UTF-8")
      .mkString
      .replace("https://petstore.swagger.io/v2/swagger.json", swaggerURL.toString)

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

  private def retrieveSwagger(context: RoutingContext): Unit = {
    val (scheme, host, basePath) = parseAbsoluteURI(context.request())

    val is = getClass.getResourceAsStream(s"/swagger.json")
    val file = Source.fromInputStream(is, "UTF-8").mkString

    val json = file
      .replace("$SCHEME$", scheme)
      .replace("$HOST$", host)
      .replace("$BASEPATH$", if (basePath.startsWith("/")) basePath else s"/$basePath")

    sendReply(context, OkString(json, "application/javascript; charset=UTF-8"))
  }

  private def retrieveFile(context: RoutingContext): Unit = {
    for {
      file <- getStringParam("file", context)
    } yield sendReply(context, SendEmbeddedFile(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/$file"))
  }

  private def retrieveFileWithDirectory(context: RoutingContext): Unit = {
    for {
      directory <- getStringParam("directory", context)
      file <- getStringParam("file", context)
    } yield sendReply(
      context,
      SendEmbeddedFile(s"/META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/$directory/$file")
    )
  }

  private def parseAbsoluteURI(request: HttpServerRequest): (String, String, String) = {

    /**
      * Sometimes we use tableaux behind some weird proxy configurations If so we use x-forwarded headers to figure out
      * how to point to swagger json
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
    val openApiUrl = config.openApiUrl

    val uri = (forwardedScheme, forwardedHost, forwardedUrl, openApiUrl) match {
      case (Some(scheme), Some(host), Some(query), _) => s"$scheme://$host$query"
      case (_, _, _, Some(openApiUrl)) => openApiUrl
      case _ => request.absoluteURI()
    }

    DocUriParser.parse(uri)
  }
}
