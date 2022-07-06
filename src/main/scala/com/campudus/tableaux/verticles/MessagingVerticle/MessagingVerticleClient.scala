package com.campudus.tableaux.verticles.MessagingVerticle

import io.vertx.scala.core.Vertx
import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId, RowId}
import com.campudus.tableaux.verticles.Messaging.MessagingVerticle
import org.vertx.scala.core.json.Json
import io.vertx.scala.core.eventbus.Message
import scala.concurrent.Future

object MessagingVerticleClient {

  def apply(vertx: Vertx): MessagingVerticleClient = {
    new MessagingVerticleClient(vertx)
  }
}

class MessagingVerticleClient(val vertx: Vertx) extends VertxAccess {
  val eventBus = vertx.eventBus()

  def cellChanged(tableId: TableId, rowId: RowId): Future[Unit] = {
    val message = Json.obj("tableId" -> tableId, "rowId" -> rowId)
    eventBus.sendFuture[String](MessagingVerticle.ADDRESS_CELL_CHANGED, message).map((f: Message[String]) => {})
  }
}
