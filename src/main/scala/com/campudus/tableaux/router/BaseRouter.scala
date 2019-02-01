package com.campudus.tableaux.router

import java.io.FileNotFoundException
import java.net.URLEncoder

import com.campudus.tableaux._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.{EmptyReturn, GetReturn, ReturnType}
import com.campudus.tableaux.helper._
import com.typesafe.scalalogging.LazyLogging
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.DecodeException
import io.vertx.scala.FutureHelper._
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.http.{HttpServerRequest, HttpServerResponse}
import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.core.json._

import scala.concurrent.{Future, Promise}
import scala.io.Source
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait BaseRouter extends VertxAccess with LazyLogging {

  protected val TABLE_ID = """(?<tableId>[\d]+)"""
  protected val COLUMN_ID = """(?<columnId>[\d]+)"""
  protected val ROW_ID = """(?<rowId>[\d]+)"""
  protected val LINK_ID = """(?<linkId>[\d]+)"""
  protected val GROUP_ID = """(?<groupId>[\d]+)"""

  val config: TableauxConfig

  override val vertx: Vertx = config.vertx

  /**
    * Regex for a UUID Version 4
    */
  val uuidRegex: String = """(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})"""

  /**
    * Regex for a Language tag e.g. de, en, de_DE, de-DE, or en_GB
    */
  val langtagRegex: String = """(?<langtag>[a-z]{2,3}|[a-z]{2,3}[-_][A-Z]{2,3})"""

  /**
    * Base result JSON
    */
  val baseResult = Json.obj("status" -> "ok")

  type AsyncReplyFunction = (=> Future[DomainObject]) => AsyncReply

  def asyncGetReply: AsyncReplyFunction = asyncReply(GetReturn)(_)

  def asyncEmptyReply: AsyncReplyFunction = asyncReply(EmptyReturn)(_)

  private def asyncReply(returnType: ReturnType)(replyFunction: => Future[DomainObject]): AsyncReply = {
    AsyncReply {
      val catchedReplyFunction = Try(replyFunction) match {
        case Success(future) => future
        case Failure(ex) => Future.failed(ex)
      }

      catchedReplyFunction
        .map({ result =>
          Ok(result.toJson(returnType).mergeIn(baseResult))
        })
        .map({ reply =>
          Header("Expires", "-1", Header("Cache-Control", "no-cache", reply))
        })
        .recover({
          case ex: InvalidNonceException => Error(RouterException(ex.message, null, ex.id, ex.statusCode))
          case ex: NoNonceException => Error(RouterException(ex.message, null, ex.id, ex.statusCode))
          case ex: CustomException => Error(ex.toRouterException)
          case ex: IllegalArgumentException => Error(RouterException(ex.getMessage, ex, "error.arguments", 422))
          case NonFatal(ex) => Error(RouterException("unknown error", ex, "error.unknown", 500))
        })
    }
  }

  def getJson(context: RoutingContext): Future[JsonObject] = futurify { p: Promise[JsonObject] =>
    context.request().bodyHandler(parseRequestBuffer(p))
  }

  private def parseRequestBuffer(p: Promise[JsonObject])(buffer: Buffer): Unit = {
    val requestBody = buffer.toString()
    Option(requestBody).getOrElse("").isEmpty match {
      case true =>
        p.failure(NoJsonFoundException("No JSON found."))

      case false =>
        requestBody match {
          case "null" =>
            p.success(Json.emptyObj())

          case _ =>
            Try(Json.fromObjectString(requestBody)) match {
              case Success(r) =>
                p.success(r)

              case Failure(ex: DecodeException) =>
                logger.error(s"Couldn't parse requestBody. JSON is valid: [$requestBody]")
                p.failure(InvalidJsonException(ex.getMessage, "invalid"))

              case Failure(ex) =>
                logger.error(s"Couldn't parse requestBody. Excepted JSON but got: [$requestBody]")
                p.failure(ex)
            }
        }
    }
  }

  def getLongParam(name: String, context: RoutingContext): Option[Long] = {
    context.request().getParam(name).map(_.toLong)
  }

  def getStringParam(name: String, context: RoutingContext): Option[String] = {
    context.request().getParam(name)
  }

  def getStringParams(name: String, context: RoutingContext): Seq[String] = {
    context.request().params().getAll(name)
  }

  def getStringCookie(name: String, context: RoutingContext): Option[String] = {
    context.getCookie(name).map(_.getValue())
  }

  def getTableId(context: RoutingContext): Option[Long] = {
    getLongParam("tableId", context)
  }

  def getColumnId(context: RoutingContext): Option[Long] = {
    getLongParam("columnId", context)
  }

  def getRowId(context: RoutingContext): Option[Long] = {
    getLongParam("rowId", context)
  }

  def getLinkId(context: RoutingContext): Option[Long] = {
    getLongParam("linkId", context)
  }

  def getGroupId(context: RoutingContext): Option[Long] = {
    getLongParam("groupId", context)
  }

  def getLangtag(context: RoutingContext): Option[String] = {
    getStringParam("langtag", context)
  }

  def getUUID(context: RoutingContext): Option[String] = {
    getStringParam("uuid", context)
  }

  def noRouteMatched(context: RoutingContext): Unit = {
    sendReply(
      context,
      Error(
        RouterException(message =
                          s"No route found for path ${context.request().method().toString} ${context.normalisedPath()}",
                        id = "NOT FOUND",
                        statusCode = 404))
    )
  }

  def defaultRoute(context: RoutingContext): Unit = {
    sendReply(context, SendEmbeddedFile("/index.html"))
  }

  /** The working directory */
  protected def workingDirectory: String = "./"

  /** File to send if the given file in SendFile was not found. */
  protected def notFoundFile: String = "404.html"

  private def fileExists(file: String): Future[String] = {
    vertx
      .fileSystem()
      .existsFuture(file)
      .flatMap({
        case true => Future.successful(file)
        case false => Future.failed(new FileNotFoundException(file))
      })
  }

  private def addIndexToDirName(path: String): String = {
    if (path.endsWith("/")) path + "index.html"
    else {
      path + "/index.html"
    }
  }

  private def directoryToIndexFile(path: String): Future[String] = {
    vertx
      .fileSystem()
      .lpropsFuture(path)
      .flatMap({ fp =>
        if (fp.isDirectory) {
          fileExists(addIndexToDirName(path))
        } else {
          Future.successful(path)
        }
      })
  }

  private def urlEncode(str: String) = URLEncoder.encode(str, "UTF-8")

  private def endResponse(resp: HttpServerResponse, reply: SyncReply): Unit = {
    reply match {
      case NoBody =>
        resp.end()
      case OkString(string, contentType) =>
        resp.setStatusCode(200)
        resp.setStatusMessage("OK")
        resp.putHeader("Content-type", contentType)
        resp.end(string)
      case Ok(js) =>
        resp.setStatusCode(200)
        resp.setStatusMessage("OK")
        resp.putHeader("Content-type", "application/json")
        resp.end(js.encode())
      case SendEmbeddedFile(path) =>
        try {
          resp.setStatusCode(200)
          resp.setStatusMessage("OK")

          val extension = if (path.contains(".")) {
            path.split("\\.").toList.last.toLowerCase()
          } else {
            "other"
          }

          val byteResponse = extension match {
            case "html" =>
              resp.putHeader("Content-type", "text/html; charset=UTF-8")
              false
            case "js" =>
              resp.putHeader("Content-type", "application/javascript; charset=UTF-8")
              false
            case "json" =>
              resp.putHeader("Content-type", "application/json; charset=UTF-8")
              false
            case "css" =>
              resp.putHeader("Content-type", "text/css; charset= UTF-8")
              false
            case "png" =>
              resp.putHeader("Content-type", "image/png")
              true
            case "gif" =>
              resp.putHeader("Content-type", "image/gif")
              true
            case _ | "txt" =>
              resp.putHeader("Content-type", "text/plain; charset= UTF-8")
              false
          }

          val is = getClass.getResourceAsStream(path)

          if (byteResponse) {
            val bytes = Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray
            resp.end(Buffer.buffer(bytes))
          } else {
            val file = Source.fromInputStream(is, "UTF-8").mkString
            resp.end(file)
          }
        } catch {
          case ex: Throwable =>
            endResponse(
              resp,
              Error(RouterException("send embedded file exception", ex, "errors.routing.sendEmbeddedFile", 500)))
        }
      case SendFile(path, absolute) =>
        (for {
          exists <- fileExists(if (absolute) path else s"$workingDirectory/$path")
          file <- directoryToIndexFile(exists)
        } yield {
          logger.info(s"Serving file $file after receiving request for: $path")
          resp.sendFile(file)
        }) recover {
          case ex: FileNotFoundException =>
            endResponse(resp, Error(RouterException("File not found", ex, "errors.routing.fileNotFound", 404)))
          case ex =>
            endResponse(resp, Error(RouterException("send file exception", ex, "errors.routing.sendFile", 500)))
        }
      case Error(RouterException(message, cause, id, 404)) =>
        logger.warn(s"Error 404: $message", cause)
        resp.setStatusCode(404)
        resp.setStatusMessage("NOT FOUND")
        message match {
          case null => resp.end()
          case msg => resp.end(msg)
        }
      case Error(RouterException(message, cause, id, statusCode)) =>
        logger.warn(s"Error $statusCode: $message", cause)
        resp.setStatusCode(statusCode)
        resp.setStatusMessage(id)
        message match {
          case null => resp.end()
          case msg => resp.end(msg)
        }
    }
  }

  def sendReply(context: RoutingContext, reply: Reply): Unit = {
    logger.debug(s"Sending back reply as response: $reply")
    val req = context.request()

    reply match {
      case AsyncReply(future) =>
        future.onComplete {
          case Success(r) => sendReply(context, r)
          case Failure(x: RouterException) => endResponse(req.response(), errorReplyFromException(x))
          case Failure(x: Throwable) => endResponse(req.response(), Error(routerException(x)))
        }
      case SetCookie(key, value, nextReply) =>
        req.response().headers().add("Set-Cookie", s"${urlEncode(key)}=${urlEncode(value)}")
        sendReply(context, nextReply)
      case Header(key, value, nextReply) =>
        req.response().putHeader(key, value)
        sendReply(context, nextReply)
      case StatusCode(statusCode, nextReply) =>
        req.response().setStatusCode(statusCode)
        sendReply(context, nextReply)
      case x: SyncReply => endResponse(req.response(), x)
    }
  }

  private def routerException(ex: Throwable): RouterException = ex match {
    case x: RouterException => x
    case x => RouterException(message = x.getMessage, cause = x)
  }

  private def errorReplyFromException(ex: RouterException) = Error(ex)

//  /**
//    * To be able to use this in `HttpServer.requestHandler()`, the Router needs to be a `HttpServerRequest => Unit`. This
//    * apply method starts the magic to be able to use `override def request() = ...` for the routes.
//    */
//  final def apply(context: RoutingContext): Unit = {
//    val req = context.request()
//    logger.info(s"${req.method()}-Request: ${req.uri()}")
//  }
}

/**
  * @author <a href="http://www.campudus.com/">Joern Bernhardt</a>
  */
case class RouterException(
    message: String = "",
    cause: Throwable = null,
    id: String = "UNKNOWN_SERVER_ERROR",
    statusCode: Int = 500
) extends Exception(message, cause)
