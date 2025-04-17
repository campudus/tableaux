package com.campudus.tableaux.router

import com.campudus.tableaux._
import com.campudus.tableaux.database.{EmptyReturn, GetReturn, ReturnType}
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.helper._
import com.campudus.tableaux.router.auth.permission.{ComparisonObjects, RoleModel, TableauxUser}

import io.vertx.core.buffer.Buffer
import io.vertx.core.json.DecodeException
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.http.HttpServerResponse
import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.core.json._

import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import java.io.{File, FileNotFoundException}
import java.net.URLEncoder

trait BaseRouter extends VertxAccess {

  protected val tableId = """(?<tableId>[\d]+)"""
  protected val columnId = """(?<columnId>[\d]+)"""
  protected val rowId = """(?<rowId>[\d]+)"""
  protected val linkId = """(?<linkId>[\d]+)"""
  protected val groupId = """(?<groupId>[\d]+)"""

  /**
    * Regex for a UUID Version 4
    */
  protected val uuidRegex: String =
    """(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})"""

  /**
    * Regex for a Language tag e.g. de, en, de_DE, de-DE, or en_GB
    */
  protected val langtagRegex: String = """(?<langtag>[a-z]{2,3}|[a-z]{2,3}[-_][A-Z]{2,3})"""

  val config: TableauxConfig

  val roleModel: RoleModel = RoleModel(config.rolePermissions)

  override val vertx: Vertx = config.vertx

  /**
    * Base result JSON
    */
  val baseResult: JsonObject = Json.obj("status" -> "ok")

  def asyncGetReply(replFunc: => Future[DomainObject])(implicit user: TableauxUser): AsyncReply = {
    asyncReply(GetReturn)(replFunc)
  }

  def asyncEmptyReply(replFunc: => Future[DomainObject])(implicit user: TableauxUser): AsyncReply = {
    asyncReply(EmptyReturn)(replFunc)
  }

  private def enrich(obj: DomainObject, returnType: ReturnType)(implicit user: TableauxUser): JsonObject = {
    val resultJson = obj.toJson(returnType)
    obj match {
      case col: ConcatenateColumn => {
        val concatsWithPermissions = col.columns.map(concatCol => enrich(concatCol, returnType))
        resultJson.mergeIn(Json.obj("concats" -> concatsWithPermissions))
      }
      case col: ColumnType[_] =>
        roleModel.enrichColumn(resultJson, ComparisonObjects(col.table, col))
      case colSeq: ColumnSeq => {
        val seqJson = roleModel.enrichColumnSeq(resultJson)
        val columns = colSeq.columns.map(col => enrich(col, returnType))
        seqJson.mergeIn(Json.obj("columns" -> columns))
      }
      case _: ExtendedFolder => roleModel.enrichMedia(resultJson)
      case _: Service => roleModel.enrichService(resultJson)
      case _: ServiceSeq => roleModel.enrichServiceSeq(resultJson)
      case table: Table => roleModel.enrichTable(resultJson, ComparisonObjects(table))
      case tableSeq: TableSeq => {
        val seqJson = roleModel.enrichTableSeq(resultJson)
        val tables = tableSeq.tables.map(table => enrich(table, returnType))
        seqJson.mergeIn(Json.obj("tables" -> tables))
      }
      case compTable: CompleteTable => {
        val tableWithPermissions = enrich(compTable.table, returnType)
        val columnsWithPermissions = compTable.columns.map(col => enrich(col, returnType))
        resultJson.mergeIn(tableWithPermissions).mergeIn(Json.obj("columns" -> columnsWithPermissions))
      }
      case _ => resultJson
    }
  }

  private def asyncReply(returnType: ReturnType)(replyFunction: => Future[DomainObject])(implicit
  user: TableauxUser): AsyncReply = {
    AsyncReply {
      val catchedReplyFunction = Try(replyFunction) match {
        case Success(future) => future
        case Failure(ex) => Future.failed(ex)
      }

      catchedReplyFunction
        .map({ result =>
          Ok(enrich(result, returnType)(user).mergeIn(baseResult))
        })
        .map({ reply =>
          Header("Expires", "-1", Header("Cache-Control", "no-cache", reply))
        })
        .recover({
          case ex: InvalidNonceException => Error(RouterException(ex.message, null, ex.id, ex.statusCode))
          case ex: NoNonceException => Error(RouterException(ex.message, null, ex.id, ex.statusCode))
          case ex: CustomException => Error(ex.toRouterException)
          case ex: IllegalArgumentException => Error(RouterException(ex.getMessage, ex, "error.arguments", 422))
          case NonFatal(ex) => Error(RouterException("Unknown error", ex, "error.unknown", 500))
        })
    }
  }

  def getJson(context: RoutingContext): JsonObject = {

    val buffer = context.getBody().map(_.toString()).getOrElse("")

    if (buffer.isEmpty) {
      throw NoJsonFoundException("No JSON found.")
    }

    buffer match {
      case "null" => Json.emptyObj()

      case _ =>
        Try(Json.fromObjectString(buffer)) match {
          case Success(r) => r

          case Failure(ex: DecodeException) =>
            logger.error(s"Couldn't parse requestBody. JSON is invalid: [$buffer]")
            throw InvalidJsonException(ex.getMessage, "invalid")

          case Failure(ex) =>
            logger.error(s"Couldn't parse requestBody. Expected JSON but got: [$buffer]")
            throw ex
        }
    }
  }

  protected def getNullableObject(field: String)(implicit json: JsonObject): Option[JsonObject] = {
    Option(json.getJsonObject(field))
  }

  protected def getNullableLong(field: String)(implicit json: JsonObject): Option[Long] = {
    Option(json.getLong(field)).map(_.toLong)
  }

  def getLongParam(name: String, context: RoutingContext): Option[Long] = {
    context.request().getParam(name).map(_.toLong)
  }

  def getBoolParam(name: String, context: RoutingContext): Option[Boolean] = {
    context.request().getParam(name).map(_.toBoolean)
  }

  def getBoolQuery(name: String, context: RoutingContext): Option[Boolean] = {
    context.queryParams().get(name).map(_.toBoolean)
  }

  def getIntQuery(name: String, context: RoutingContext): Option[Int] = {
    context.queryParams().get(name).map(_.toInt)
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

  protected def getTableId(context: RoutingContext): Option[Long] = {
    getLongParam("tableId", context)
  }

  protected def getColumnId(context: RoutingContext): Option[Long] = {
    getLongParam("columnId", context)
  }

  protected def getLangtag(context: RoutingContext): Option[String] = {
    getStringParam("langtag", context)
  }

  protected def getUUID(context: RoutingContext): Option[String] = {
    getStringParam("uuid", context)
  }

  def noRouteMatched(context: RoutingContext): Unit = {
    sendReply(
      context,
      Error(
        RouterException(
          message =
            s"No route found for path ${context.request().method().toString} ${context.normalisedPath()}",
          id = "NOT FOUND",
          statusCode = 404
        )
      )
    )
  }

  def defaultRoute(context: RoutingContext): Unit = {
    sendReply(context, SendEmbeddedFile("/index.html"))
  }

  /** The working directory */
  protected def workingDirectory: String = "./"

  /** File to send if the given file in SendFile was not found. */
  protected def notFoundFile: String = "404.html"

  private def checkExistence(file: String): Future[String] = {
    vertx
      .fileSystem()
      .existsFuture(file)
      .flatMap({
        case true => Future.successful(file)
        case false => Future.failed(new FileNotFoundException(file))
      })
  }

  private def addIndexToDirName(path: String): String = {
    if (path.endsWith("/")) {
      path + "index.html"
    } else {
      path + "/index.html"
    }
  }

  private def directoryToIndexFile(path: String): Future[String] = {
    vertx
      .fileSystem()
      .lpropsFuture(path)
      .flatMap({ fp =>
        if (fp.isDirectory) {
          checkExistence(addIndexToDirName(path))
        } else {
          Future.successful(path)
        }
      })
  }

  private def urlEncode(str: String): String = URLEncoder.encode(str, "UTF-8")

  private def endResponse(resp: HttpServerResponse, reply: SyncReply): Unit = {
    reply match {
      case NoBody =>
        resp.end()
      case OkString(string, contentType) =>
        resp.setStatusCode(200)
        resp.setStatusMessage("OK")
        resp.putHeader("Content-type", contentType)
        resp.end(string)
      case OkBuffer(buffer, contentType) =>
        resp.setStatusCode(200)
        resp.setStatusMessage("OK")
        resp.putHeader("Content-type", contentType)
        resp.end(buffer)
      case Ok(js) =>
        resp.setStatusCode(200)
        resp.setStatusMessage("OK")
        resp.putHeader("Content-type", "application/json")
        resp.end(js.encode())
      case SendEmbeddedFile(path) =>
        try {
          resp.setStatusCode(200)
          resp.setStatusMessage("OK")

          val extension: String =
            if (path.contains(".")) {
              path.split("\\.").toList.lastOption.map(_.toLowerCase).getOrElse("")
            } else {
              "other"
            }

          def switchContentType(ext: String): (String, Boolean) = {
            ext match {
              case "html" => ("text/html; charset=UTF-8", false)
              case "js" => ("application/javascript; charset=UTF-8", false)
              case "json" => ("application/json; charset=UTF-8", false)
              case "css" => ("text/css; charset= UTF-8", false)
              case "png" => ("image/png", true)
              case "gif" => ("image/gif", true)
              case _ | "txt" => ("text/plain; charset= UTF-8", false)
            }
          }

          val (contentType, byteResponse) = switchContentType(extension)

          resp.putHeader("Content-type", contentType)

          val inputStream = getClass.getResourceAsStream(path)

          if (byteResponse) {
            val bytes = Stream.continually(inputStream.read).takeWhile(_ != -1).map(_.toByte).toArray
            resp.end(Buffer.buffer(bytes))
          } else {
            val file = Source.fromInputStream(inputStream, "UTF-8").mkString
            resp.end(file)
          }
        } catch {
          case NonFatal(ex) =>
            endResponse(
              resp,
              Error(RouterException("Send embedded file exception", ex, "errors.routing.sendEmbeddedFile"))
            )
        }
      case SendFile(path, absolute) =>
        {
          val filePath =
            if (absolute) {
              path
            } else {
              new File(workingDirectory, path).toString
            }

          for {
            _ <- checkExistence(filePath)
            file <- directoryToIndexFile(filePath)
          } yield {
            logger.info(s"Serving file $file after receiving request for: $path")
            resp.sendFile(file)

          }
        } recover {
          case ex: FileNotFoundException =>
            endResponse(resp, Error(RouterException("File not found", ex, "errors.routing.fileNotFound", 404)))
          case ex =>
            endResponse(resp, Error(RouterException("Send file exception", ex, "errors.routing.sendFile")))
        }
      case Error(RouterException(message, cause, id, 404)) =>
        logger.warn(s"Error 404: $message", cause)
        resp.setStatusCode(404)
        resp.setStatusMessage("NOT FOUND")
        Option(message).fold(resp.end())(resp.end)
      case Error(RouterException(message, cause, id, statusCode)) =>
        logger.warn(s"Error $statusCode: $message", cause)
        resp.setStatusCode(statusCode)
        resp.setStatusMessage(id)
        Option(message).fold(resp.end())(resp.end)
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
}

/**
  * @author
  *   <a href="http://www.campudus.com/">Joern Bernhardt</a>
  */
case class RouterException(
    message: String = "",
    cause: Throwable = null,
    id: String = "UNKNOWN_SERVER_ERROR",
    statusCode: Int = 500
) extends Exception(message, cause)
