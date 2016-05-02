package com.campudus.tableaux.cache

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import io.vertx.core.eventbus.{EventBus, Message}
import io.vertx.core.{AsyncResult, Handler, Vertx}
import io.vertx.scala.VertxExecutionContext
import org.vertx.scala.core.json._

import scala.concurrent.Future

object CacheClient {
  def apply(vertx: Vertx): CacheClient = {
    new CacheClient(vertx: Vertx)
  }
}

class CacheClient(override val _vertx: Vertx) extends VertxExecutionContext {

  import io.vertx.scala.FunctionConverters._

  type AsyncMessage[A] = Handler[AsyncResult[Message[A]]]

  private def key(tableId: TableId, columnId: ColumnId, rowId: RowId) = {
    Json.obj(
      "tableId" -> tableId,
      "columnId" -> columnId,
      "rowId" -> rowId
    )
  }

  def setCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId, value: JsonObject)(implicit eventBus: EventBus): Future[Unit] = {
    val obj = key(tableId, columnId, rowId).copy().mergeIn(Json.obj("value" -> value))

    //(eventBus.send(CacheVerticle.ADDRESS_SET, obj, _: AsyncMessage[JsonObject]))
    //  .map(_ => ())

    eventBus.send(CacheVerticle.ADDRESS_SET, obj)

    Future.successful(())
  }

  def retrieveCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId)(implicit eventbus: EventBus): Future[Option[JsonObject]] = {
    val obj = key(tableId, columnId, rowId)

    (eventbus.send(CacheVerticle.ADDRESS_RETRIEVE, obj, _: AsyncMessage[JsonObject]))
      .map({
        v => Some(v.body())
      })
      .recoverWith({
        case _ => Future.successful(None)
      })
  }

  def invalidateCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId)(implicit eventBus: EventBus): Future[Unit] = {
    val obj = key(tableId, columnId, rowId)

    (eventBus.send(CacheVerticle.ADDRESS_INVALIDATE, obj, _: AsyncMessage[JsonObject]))
      .map(_ => ())
  }

  def invalidateColumn(tableId: TableId, columnId: ColumnId)(implicit eventBus: EventBus): Future[Unit] = {
    val obj = Json.obj("tableId" -> tableId, "columnId" -> columnId)

    (eventBus.send(CacheVerticle.ADDRESS_INVALIDATE, obj, _: AsyncMessage[JsonObject]))
      .map(_ => ())
  }
}