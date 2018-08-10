package com.campudus.tableaux.cache

import com.campudus.tableaux.database.domain.DomainObject
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.helper.VertxAccess
import io.vertx.core.eventbus.ReplyException
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus._
import org.vertx.scala.core.json._

import scala.concurrent.Future

object CacheClient {

  def apply(vertxAccess: VertxAccess): CacheClient = {
    new CacheClient(vertxAccess)
  }
}

class CacheClient(vertxAccess: VertxAccess) extends VertxAccess {

  override val vertx: Vertx = vertxAccess.vertx

  private val eventBus = vertx.eventBus()

  private def key(tableId: TableId, columnId: ColumnId, rowId: RowId) = {
    Json.obj(
      "tableId" -> tableId,
      "columnId" -> columnId,
      "rowId" -> rowId
    )
  }

  def setCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId, value: Any): Future[_] = {
    val obj = key(tableId, columnId, rowId).copy().mergeIn(Json.obj("value" -> DomainObject.compatibilityGet(value)))

    val options = DeliveryOptions()
      .setSendTimeout(CacheVerticle.TIMEOUT_AFTER_MILLISECONDS)

    eventBus
      .sendFuture(CacheVerticle.ADDRESS_SET, obj, options)
  }

  def retrieveCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Option[Any]] = {
    val obj = key(tableId, columnId, rowId)

    val options = DeliveryOptions()
      .setSendTimeout(CacheVerticle.TIMEOUT_AFTER_MILLISECONDS)

    eventBus
      .sendFuture[JsonObject](CacheVerticle.ADDRESS_RETRIEVE, obj, options)
      .map({
        case v if v.body().containsKey("value") =>
          Some(v.body().getValue("value"))
        case _ =>
          None
      })
      .recoverWith({
        case ex: ReplyException if ex.failureCode() == CacheVerticle.NOT_FOUND_FAILURE =>
          Future.successful(None)
        case ex =>
          Future.failed(ex)
      })
  }

  def invalidateCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[_] = {
    val obj = key(tableId, columnId, rowId)

    val options = DeliveryOptions()
      .setSendTimeout(CacheVerticle.TIMEOUT_AFTER_MILLISECONDS)

    eventBus
      .sendFuture(CacheVerticle.ADDRESS_INVALIDATE_CELL, obj, options)
  }

  def invalidateColumn(tableId: TableId, columnId: ColumnId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId, "columnId" -> columnId)

    eventBus
      .sendFuture(CacheVerticle.ADDRESS_INVALIDATE_COLUMN, obj)
  }

  def invalidateRow(tableId: TableId, rowId: RowId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId, "rowId" -> rowId)

    eventBus
      .sendFuture(CacheVerticle.ADDRESS_INVALIDATE_ROW, obj)
  }

  def invalidateTable(tableId: TableId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId)

    eventBus
      .sendFuture(CacheVerticle.ADDRESS_INVALIDATE_TABLE, obj)
  }

  def invalidateAll(): Future[_] = {
    eventBus
      .sendFuture(CacheVerticle.ADDRESS_INVALIDATE_ALL, Json.emptyObj())
  }
}
