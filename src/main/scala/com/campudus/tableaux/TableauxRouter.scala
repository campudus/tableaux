package com.campudus.tableaux

import com.campudus.tableaux.database.Mapper
import org.vertx.scala.router.Router
import org.vertx.scala.core.VertxAccess
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.router.routing._
import scala.concurrent.{ Future, Promise }
import org.vertx.scala.router.RouterException
import org.vertx.scala.core.json.{ Json, JsonObject, JsonArray }
import scala.util.{ Success, Failure }
import com.campudus.tableaux.database.DomainObject
import com.campudus.tableaux.HelperFunctions._

class TableauxRouter(verticle: Starter) extends Router with VertxAccess {
  val container = verticle.container
  val vertx = verticle.vertx
  val logger = verticle.logger

  val tableIdColumnsIdRowsId = "/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)".r
  val tableIdColumnsId = "/tables/(\\d+)/columns/(\\d+)".r
  val tableIdColumns = "/tables/(\\d+)/columns".r
  val tableIdRowsId = "/tables/(\\d+)/rows/(\\d+)".r
  val tableIdRows = "/tables/(\\d+)/rows".r
  val tableId = "/tables/(\\d+)".r
  val controller = new TableauxController(verticle)

  def routes(implicit req: HttpServerRequest): PartialFunction[RouteMatch, Reply] = {
    case Get("/") => SendFile("index.html")
    case Get(tableId(tableId)) => getAsyncReply(controller.getTable(tableId.toLong))
    case Get(tableIdColumnsId(tableId, columnId)) => getAsyncReply(controller.getColumn(tableId.toLong, columnId.toLong))
    case Get(tableIdRowsId(tableId, rowId)) => getAsyncReply(controller.getRow(tableId.toLong, rowId.toLong))
    case Get(tableIdColumnsIdRowsId(tableId, columnId, rowId)) => getAsyncReply(controller.getCell(tableId.toLong, columnId.toLong, rowId.toLong))
    case Post("/reset") => getAsyncReply(controller.resetDB())
    case Post("/tables") => getAsyncReply {
      getJson(req) flatMap { json => controller.createTable(json.getString("tableName")) }
    }
    case Post(tableIdColumns(tableId)) => getAsyncReply {
      for {
        json <- getJson(req)
        dbType <- Future.apply(Mapper.getDatabaseType(json.getArray("type").get[String](0))) recoverWith {
          case _ => Future.failed(NotEnoughArgumentsException("Warning: Not enough Arguments", "arguments"))
        }
        x <- dbType match {
          case "link" => controller.createColumn(tableId.toLong, json.getString("columnName"), dbType, json.getLong("toTable"), json.getLong("toColumn"), json.getLong("fromColumn"))
          case _ => controller.createColumn(tableId.toLong, jsonToSeqOfColumnNameAndType(json))
        }
      } yield x
    }
    case Post(tableIdRows(tableId)) => getAsyncReply {
      for {
        opt <- getJson(req) map { json =>
          Some(jsonToSeqOfRowsWithColumnIdAndValue(json))
        } recover { case ex: NoJsonFoundException => None }
        res <- controller.createRow(tableId.toLong, opt)
      } yield res
    }
    case Post(tableIdColumnsIdRowsId(tableId, columnId, rowId)) => getAsyncReply {
      getJson(req) flatMap {
        json => controller.fillCell(tableId.toLong, columnId.toLong, rowId.toLong, Mapper.getDatabaseType(json.getString("type")), json.getField("value"))
      }
    }
    case Delete(tableId(tableId)) => getAsyncReply(controller.deleteTable(tableId.toLong))
    case Delete(tableIdColumnsId(tableId, columnId)) => getAsyncReply(controller.deleteColumn(tableId.toLong, columnId.toLong))
    case Delete(tableIdRowsId(tableId, rowId)) => getAsyncReply(controller.deleteRow(tableId.toLong, rowId.toLong))
  }

  private def getAsyncReply(f: => Future[DomainObject]): AsyncReply = AsyncReply {
    f map { d => Ok(d.toJson) } recover {
      case ex @ NotFoundInDatabaseException(message, id) => Error(RouterException(message, ex, s"errors.database.$id", 404))
      case ex @ DatabaseException(message, id) => Error(RouterException(message, ex, s"errors.database.$id", 500))
      case ex @ NoJsonFoundException(message, id) => Error(RouterException(message, ex, s"errors.json.$id", 400))
      case ex @ NotEnoughArgumentsException(message, id) => Error(RouterException(message, ex, s"error.json.$id", 400))
      case ex: Throwable => Error(RouterException("unknown error", ex, "errors.unknown", 500))
    }
  }

  private def getJson(req: HttpServerRequest): Future[JsonObject] = {
    val p = Promise[JsonObject]
    req.bodyHandler { buf =>
      buf.length() match {
        case 0 => p.failure(NoJsonFoundException("Warning: No Json found", "not-found"))
        case _ => p.success(Json.fromObjectString(buf.toString()))
      }
    }
    p.future
  }
}
