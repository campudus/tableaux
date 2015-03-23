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
import com.campudus.tableaux.database._

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

  val demoJson = Json.obj(
    "tableName" -> "Regierungsbezirke Bayern",
    "columns" -> Json.arr(
      Json.obj("kind" -> "text", "name" -> "Regierungsbezirk", "ordering" -> 1),
      Json.obj("kind" -> "text", "name" -> "Hauptstadt", "ordering" -> 2),
      Json.obj("kind" -> "numeric", "name" -> "Einwohner", "ordering" -> 3),
      Json.obj("kind" -> "numeric", "name" -> "Fläche in km²", "ordering" -> 4)),
    "rows" -> Json.arr(
      Json.obj("values" -> Json.arr("Oberbayern", "München", 4469342, 17530.21)),
      Json.obj("values" -> Json.arr("Niederbayern", "Landshut", 1189153, 10328.65)),
      Json.obj("values" -> Json.arr("Oberpfalz", "Regensburg", 1077991, 9690.19)),
      Json.obj("values" -> Json.arr("Oberfranken", "Bayreuth", 1056365, 7231.49))))

  val demoJson2 = Json.obj(
    "tableName" -> "Bundesländer Deutschlands",
    "columns" -> Json.arr(
      Json.obj("kind" -> "text", "name" -> "Land", "ordering" -> 1),
      Json.obj("kind" -> "text", "name" -> "Hauptstadt", "ordering" -> 2),
      Json.obj("kind" -> "numeric", "name" -> "Fläche in km²", "ordering" -> 3),
      Json.obj("kind" -> "numeric", "name" -> "Einwohner", "ordering" -> 4)),
    "rows" -> Json.arr(
      Json.obj("values" -> Json.arr("Baden-Württemberg", "Stuttgart", 35751, 10631278)),
      Json.obj("values" -> Json.arr("Bayern", "München", 70550, 12604244)),
      Json.obj("values" -> Json.arr("Berlin", "Berlin", 892, 3440991)),
      Json.obj("values" -> Json.arr("Brandenburg", "Potsdam", 29655, 2449193))))

  def routes(implicit req: HttpServerRequest): PartialFunction[RouteMatch, Reply] = {
    case Get("/") => SendFile("index.html")
    case Get("/tables") => getAsyncReply(GetReturn)(controller.getAllTables())
    case Get(tableId(tableId)) => getAsyncReply(GetReturn)(controller.getTable(tableId.toLong))
    case Get(tableIdColumnsId(tableId, columnId)) => getAsyncReply(GetReturn)(controller.getColumn(tableId.toLong, columnId.toLong))
    case Get(tableIdRowsId(tableId, rowId)) => getAsyncReply(GetReturn)(controller.getRow(tableId.toLong, rowId.toLong))
    case Get(tableIdColumnsIdRowsId(tableId, columnId, rowId)) => getAsyncReply(GetReturn)(controller.getCell(tableId.toLong, columnId.toLong, rowId.toLong))
    case Post("/reset") => getAsyncReply(SetReturn)(controller.resetDB())
    case Post("/resetDemo") => getAsyncReply(SetReturn) {
      controller.createTable(demoJson.getString("tableName"), jsonToSeqOfColumnNameAndType(demoJson), jsonToSeqOfRowsWithValue(demoJson))
      controller.createTable(demoJson2.getString("tableName"), jsonToSeqOfColumnNameAndType(demoJson2), jsonToSeqOfRowsWithValue(demoJson2))
    }
    case Post("/tables") => getAsyncReply(SetReturn) {
      getJson(req) flatMap { json =>
        if (json.getFieldNames.contains("columns")) {
          if (json.getFieldNames.contains("rows")) {
            controller.createTable(json.getString("tableName"), jsonToSeqOfColumnNameAndType(json), jsonToSeqOfRowsWithValue(json))
          } else {
            controller.createTable(json.getString("tableName"), jsonToSeqOfColumnNameAndType(json), Seq())
          }
        } else {
          controller.createTable(json.getString("tableName"))
        }
      }
    }
    case Post(tableIdColumns(tableId)) => getAsyncReply(SetReturn) {
      getJson(req) flatMap (json => controller.createColumn(tableId.toLong, jsonToSeqOfColumnNameAndType(json)))
    }
    case Post(tableIdRows(tableId)) => getAsyncReply(SetReturn) {
      getJson(req) flatMap (json => controller.createRow(tableId.toLong, Some(jsonToSeqOfRowsWithColumnIdAndValue(json)))) recoverWith {
        case _: NoJsonFoundException => controller.createRow(tableId.toLong, None)
      }
    }
    case Post(tableIdColumnsIdRowsId(tableId, columnId, rowId)) => getAsyncReply(SetReturn) {
      getJson(req) flatMap {
        json => controller.fillCell(tableId.toLong, columnId.toLong, rowId.toLong, jsonToValues(json))
      }
    }
    case Post(tableId(tableId)) => getAsyncReply(EmptyReturn)(getJson(req) flatMap (json => controller.changeTableName(tableId.toLong, json.getString("tableName"))))
    case Post(tableIdColumnsId(tableId, columnId)) => getAsyncReply(EmptyReturn) {
      getJson(req) flatMap {
        json =>
          val (optName, optOrd, optKind) = getColumnChanges(json)
          controller.changeColumn(tableId.toLong, columnId.toLong, optName, optOrd, optKind)
      }
    }
    case Delete(tableId(tableId)) => getAsyncReply(EmptyReturn)(controller.deleteTable(tableId.toLong))
    case Delete(tableIdColumnsId(tableId, columnId)) => getAsyncReply(EmptyReturn)(controller.deleteColumn(tableId.toLong, columnId.toLong))
    case Delete(tableIdRowsId(tableId, rowId)) => getAsyncReply(EmptyReturn)(controller.deleteRow(tableId.toLong, rowId.toLong))
  }

  private def getAsyncReply(reType: ReturnType)(f: => Future[DomainObject]): AsyncReply = AsyncReply {
    f map { d => Ok(Json.obj("status" -> "ok").mergeIn(d.toJson(reType))) } recover {
      case ex @ NotFoundInDatabaseException(message, id) => Error(RouterException(message, ex, s"errors.database.$id", 404))
      case ex @ DatabaseException(message, id) => Error(RouterException(message, ex, s"errors.database.$id", 500))
      case ex @ NoJsonFoundException(message, id) => Error(RouterException(message, ex, s"errors.json.$id", 400))
      case ex @ NotEnoughArgumentsException(message, id) => Error(RouterException(message, ex, s"error.json.$id", 400))
      case ex @ InvalidJsonException(message, id) => Error(RouterException(message, ex, s"error.json.$id", 400))
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
