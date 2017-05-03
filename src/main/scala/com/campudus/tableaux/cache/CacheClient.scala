package com.campudus.tableaux.cache

import com.campudus.tableaux.database.domain.DomainObject
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import io.vertx.core.eventbus._
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

  private val eventBus = _vertx.eventBus()

  private def key(tableId: TableId, columnId: ColumnId, rowId: RowId) = {
    Json.obj(
      "tableId" -> tableId,
      "columnId" -> columnId,
      "rowId" -> rowId
    )
  }

  def setCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId, value: Any): Future[Unit] = {
    val encodedValue = value match {
      case v: Seq[_] =>
        v.map({
          case e: DomainObject => e.getJson
          case e => e
        })
      case v: DomainObject => v.getJson
      case _ => value
    }

    val obj = key(tableId, columnId, rowId).copy().mergeIn(Json.obj("value" -> encodedValue))

    val options = new DeliveryOptions()
      .setSendTimeout(200)

    (eventBus
      .send(CacheVerticle.ADDRESS_SET, obj, options, _: AsyncMessage[JsonObject]))
      .map(_ => ())
  }

  def retrieveCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Option[Any]] = {
    val obj = key(tableId, columnId, rowId)

    val options = new DeliveryOptions()
      .setSendTimeout(200)

    (eventBus
      .send(CacheVerticle.ADDRESS_RETRIEVE, obj, options, _: AsyncMessage[JsonObject]))
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

  def invalidateCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Unit] = {
    val obj = key(tableId, columnId, rowId)

    val options = new DeliveryOptions()
      .setSendTimeout(200)

    (eventBus
      .send(CacheVerticle.ADDRESS_INVALIDATE_CELL, obj, options, _: AsyncMessage[JsonObject]))
      .map(_ => ())
  }

  def invalidateColumn(tableId: TableId, columnId: ColumnId): Future[Unit] = {
    val obj = Json.obj("tableId" -> tableId, "columnId" -> columnId)

    (eventBus
      .send(CacheVerticle.ADDRESS_INVALIDATE_COLUMN, obj, _: AsyncMessage[JsonObject]))
      .map(_ => ())
  }

  def invalidateRow(tableId: TableId, rowId: RowId): Future[Unit] = {
    val obj = Json.obj("tableId" -> tableId, "rowId" -> rowId)

    (eventBus
      .send(CacheVerticle.ADDRESS_INVALIDATE_ROW, obj, _: AsyncMessage[JsonObject]))
      .map(_ => ())
  }

  def invalidateTable(tableId: TableId): Future[Unit] = {
    val obj = Json.obj("tableId" -> tableId)

    (eventBus
      .send(CacheVerticle.ADDRESS_INVALIDATE_TABLE, obj, _: AsyncMessage[JsonObject]))
      .map(_ => ())
  }

  def invalidateAll(): Future[Unit] = {
    (eventBus
      .send(CacheVerticle.ADDRESS_INVALIDATE_ALL, Json.emptyObj(), _: AsyncMessage[JsonObject]))
      .map(_ => ())
  }
}
