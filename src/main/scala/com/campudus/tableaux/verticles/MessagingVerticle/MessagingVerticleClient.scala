package com.campudus.tableaux.verticles.MessagingVerticle

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.verticles.Messaging.MessagingVerticle

import io.vertx.core.json.JsonObject
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.Message
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

object MessagingVerticleClient {

  def apply(vertx: Vertx): MessagingVerticleClient = {
    new MessagingVerticleClient(vertx)
  }
}

class MessagingVerticleClient(val vertx: Vertx) extends VertxAccess {
  val eventBus = vertx.eventBus()

  private def sendMessage(address: String, jsonObj: JsonObject = Json.obj()): Future[Unit] = {
    eventBus.sendFuture[String](address, jsonObj).map((f: Message[String]) => {})
  }

  def cellChanged(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Unit] = {
    val message = Json.obj("tableId" -> tableId, "rowId" -> rowId, "columnId" -> columnId)
    sendMessage(MessagingVerticle.ADDRESS_CELL_CHANGED, message)
  }

  def servicesChange(): Future[Unit] = {
    sendMessage(MessagingVerticle.ADDRESS_SERVICES_CHANGE)
  }

  def columnCreated(tableId: TableId, columnId: ColumnId): Future[Unit] = {
    val message = Json.obj("columnId" -> columnId, "tableId" -> tableId)
    sendMessage(MessagingVerticle.ADDRESS_COLUMN_CREATED, message)
  }
}
