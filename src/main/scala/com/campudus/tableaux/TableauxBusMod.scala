package com.campudus.tableaux

import org.vertx.scala.mods.ScalaBusMod
import org.vertx.scala.core.eventbus.Message
import org.vertx.scala.core.json.JsonObject
import org.vertx.scala.mods.replies._
import org.vertx.scala.platform.Verticle
import com.campudus.tableaux.database.Mapper
import scala.concurrent.Future
import com.campudus.tableaux.database.DomainObject
import com.campudus.tableaux.HelperFunctions._

class TableauxBusMod(verticle: Verticle) extends ScalaBusMod {
  val container = verticle.container
  val logger = verticle.logger
  val vertx = verticle.vertx

  val controller = new TableauxController(verticle)

  def receive(): Message[JsonObject] => PartialFunction[String, BusModReceiveEnd] = msg => {
    case "reset" => getAsyncReply(controller.resetDB())
    case "getTable" => getAsyncReply(controller.getTable(msg.body().getLong("tableId")))
    case "getColumn" => getAsyncReply(controller.getColumn(msg.body().getLong("tableId"), msg.body().getLong("columnId")))
    case "getRow" => getAsyncReply(controller.getRow(msg.body().getLong("tableId"), msg.body().getLong("rowId")))
    case "getCell" => getAsyncReply(controller.getCell(msg.body().getLong("tableId"), msg.body().getLong("columnId"), msg.body().getLong("rowId")))
    case "createTable" => getAsyncReply {
      import scala.collection.JavaConverters._
      if (msg.body().getFieldNames.asScala.toSeq.contains("cols")) {
        controller.createTable(msg.body().getString("tableName"), jsonToSeqOfColumnNameAndType(msg.body().getObject("cols")), jsonToSeqOfRowsWithColumnIdAndValue(msg.body().getObject("rows")))
      } else {
        controller.createTable(msg.body().getString("tableName"))
      }
    }
    case "createColumn" => getAsyncReply {
      val dbType = Mapper.getDatabaseType(msg.body().getString("type"))
      dbType match {
        case "link" => controller.createColumn(msg.body().getLong("tableId"), msg.body().getString("columnName"), dbType, msg.body().getLong("toTable"), msg.body().getLong("toColumn"), msg.body().getLong("fromColumn"))
        case _ => controller.createColumn(msg.body().getLong("tableId"), jsonToSeqOfColumnNameAndType(msg.body()))
      }
    }
    case "createRow" => getAsyncReply(controller.createRow(msg.body().getLong("tableId"), Option(jsonToSeqOfRowsWithColumnIdAndValue(msg.body()))))
    case "fillCell" => getAsyncReply(controller.fillCell(msg.body().getLong("tableId"), msg.body().getLong("columnId"), msg.body().getLong("rowId"), msg.body().getString("type"), msg.body().getField("value")))
    case "deleteTable" => getAsyncReply(controller.deleteTable(msg.body().getLong("tableId")))
    case "deleteColumn" => getAsyncReply(controller.deleteColumn(msg.body().getLong("tableId"), msg.body().getLong("columnId")))
    case "deleteRow" => getAsyncReply(controller.deleteRow(msg.body().getLong("tableId"), msg.body().getLong("rowId")))
    case _ => throw new IllegalArgumentException("Unknown action")
  }

  private def getAsyncReply(f: => Future[DomainObject]): AsyncReply = AsyncReply {
    f map { d => Ok(d.toJson) } recover {
      case ex @ NotFoundInDatabaseException(message, id) => Error(message, s"errors.database.$id")
      case ex @ DatabaseException(message, id) => Error(message, s"errors.database.$id")
      case ex @ NoJsonFoundException(message, id) => Error(message, s"errors.json.$id")
      case ex @ NotEnoughArgumentsException(message, id) => Error(message, s"errors.json.$id")
      case ex @ InvalidJsonException(message, id) => Error(message, s"error.json.$id")
      case ex: Throwable => Error("unknown error", "errors.unknown")
    }
  }

}
