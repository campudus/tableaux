package com.campudus.tableaux.verticles.Messaging

import io.vertx.lang.scala.ScalaVerticle
import com.typesafe.scalalogging.LazyLogging
import io.vertx.scala.core.eventbus.Message
import io.vertx.core.json.JsonObject
import scala.concurrent.Future
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.RequestContext
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.campudus.tableaux.database.model.TableauxModel
import io.vertx.scala.SQLConnection
import io.vertx.scala.core.http.HttpServer
import org.vertx.scala.core.json.Json
import com.campudus.tableaux.helper.VertxAccess
import io.vertx.scala.core.Vertx
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId, RowId}

object MessagingVerticle {
  val ADDRESS_CELL_CHANGED = "message.cell.change"
}

class MessagingVerticle extends ScalaVerticle
    with LazyLogging {

  import MessagingVerticle._

  private var tableauxModel: TableauxModel = _

  private lazy val eventBus = vertx.eventBus()

  override def startFuture(): Future[_] = {
    val isAuthorization: Boolean = !config.getJsonObject("authConfig").isEmpty
    implicit val roleModel: RoleModel = RoleModel(config.getJsonObject("rolePermissions"), isAuthorization)
    implicit val requestContext: RequestContext = RequestContext()
    val vertxAccess = new VertxAccess { override val vertx: Vertx = MessagingVerticle.this.vertx }
    val connection = SQLConnection(vertxAccess, config.getJsonObject("databaseConfig"))
    val dbConnection = DatabaseConnection(vertxAccess, connection)
    val structureModel = StructureModel(dbConnection)
    tableauxModel = TableauxModel(dbConnection, structureModel)

    registerConsumers()
  }

  private def registerConsumers(): Future[Unit] = {
    eventBus.consumer(ADDRESS_CELL_CHANGED, messageHandlerCellChange).completionFuture()
  }

  private def messageHandlerCellChange(message: Message[JsonObject]): Unit = {
    val body = message.body()
    val tableId = body.getLong("tableId").asInstanceOf[TableId]
    val rowId = body.getLong("rowId").asInstanceOf[RowId]
    for {
      table <- tableauxModel.retrieveTable(tableId, true)
      dependentCells <- tableauxModel.retrieveDependentCells(table, rowId)
    } yield {
      println(dependentCells.size)
      message.reply("")
    }
  }
}
