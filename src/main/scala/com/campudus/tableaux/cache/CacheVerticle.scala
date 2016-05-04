package com.campudus.tableaux.cache

import java.util.concurrent.TimeUnit

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.google.common.cache.CacheBuilder
import io.vertx.core.eventbus.Message
import io.vertx.scala.ScalaVerticle
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.collection.mutable
import scala.concurrent.Promise
import scala.language.implicitConversions
import scalacache._
import scalacache.guava._
import scalacache.serialization.InMemoryRepr

object CacheVerticle {
  val NOT_FOUND_FAILURE = 404
  val INVALID_MESSAGE = 400

  val ADDRESS_SET = "cache.set"
  val ADDRESS_RETRIEVE = "cache.retrieve"
  val ADDRESS_INVALIDATE = "cache.invalidate"
}

class CacheVerticle extends ScalaVerticle {

  import CacheVerticle._

  lazy val eventBus = vertx.eventBus()

  val caches: mutable.Map[(TableId, ColumnId), ScalaCache[InMemoryRepr]] = mutable.Map.empty

  override def start(promise: Promise[Unit]): Unit = {
    registerOnEventBus()

    promise.success(())
  }

  override def stop(promise: Promise[Unit]): Unit = {
    promise.success(())
  }

  private def registerOnEventBus(): Unit = {
    import io.vertx.scala.FunctionConverters._

    eventBus.localConsumer(ADDRESS_SET, messageHandlerSet(_: Message[JsonObject]))
    eventBus.localConsumer(ADDRESS_RETRIEVE, messageHandlerRetrieve(_: Message[JsonObject]))
    eventBus.localConsumer(ADDRESS_INVALIDATE, messageHandlerInvalidate(_: Message[JsonObject]))
  }

  private def cache(tableId: TableId, columnId: ColumnId): ScalaCache[InMemoryRepr] = {
    def createCache() = CacheBuilder
      .newBuilder()
      .expireAfterAccess(120, TimeUnit.SECONDS)
      .maximumSize(10000L)
      .recordStats()
      .build[String, Object]

    caches.get((tableId, columnId)) match {
      case Some(cache) => cache
      case None =>
        val cache = ScalaCache(GuavaCache(createCache()))
        caches.put((tableId, columnId), cache)
        cache
    }
  }

  private def messageHandlerSet(message: Message[JsonObject]): Unit = {
    val obj = message.body()

    val value = obj.getValue("value")

    (for {
      tableId <- Option(obj.getLong("tableId")).map(_.toLong)
      columnId <- Option(obj.getLong("columnId")).map(_.toLong)
      rowId <- Option(obj.getLong("rowId")).map(_.toLong)
    } yield (tableId, columnId, rowId)) match {
      case Some((tableId, columnId, rowId)) =>

        implicit val scalaCache = cache(tableId, columnId)
        put(rowId)(value)
          .map({
            _ =>
              val reply = Json.obj(
                "tableId" -> tableId,
                "columnId" -> columnId,
                "rowId" -> rowId
              )

              message.reply(reply)
          })

      case None =>
        logger.error("Message invalid: Fields (tableId, columnId, rowId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, columnId, rowId) should be a Long")
    }
  }

  private def messageHandlerRetrieve(message: Message[JsonObject]): Unit = {
    val obj = message.body()

    (for {
      tableId <- Option(obj.getLong("tableId")).map(_.toLong)
      columnId <- Option(obj.getLong("columnId")).map(_.toLong)
      rowId <- Option(obj.getLong("rowId")).map(_.toLong)
    } yield (tableId, columnId, rowId)) match {
      case Some((tableId, columnId, rowId)) =>

        implicit val scalaCache = cache(tableId, columnId)

        get[AnyRef, NoSerialization](rowId)
          .map({
            case Some(value) =>
              val reply = Json.obj(
                "tableId" -> tableId,
                "columnId" -> columnId,
                "rowId" -> rowId,
                "value" -> value
              )

              message.reply(reply)
            case None =>

              logger.warn(s"messageHandlerRetrieve $tableId, $columnId, $rowId not found")

              message.fail(NOT_FOUND_FAILURE, "Not found")
          })

      case None =>
        logger.error("Message invalid: Fields (tableId, columnId, rowId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, columnId, rowId) should be a Long")
    }
  }

  private def messageHandlerInvalidate(message: Message[JsonObject]): Unit = {
    val obj = message.body()

    (for {
      tableId <- Option(obj.getLong("tableId")).map(_.toLong)
      columnId <- Option(obj.getLong("columnId")).map(_.toLong)
    } yield (tableId, columnId, Option(obj.getLong("rowId")).map(_.toLong))) match {
      case Some((tableId, columnId, Some(rowId))) =>

        implicit val scalaCache = cache(tableId, columnId)

        remove(rowId)
          .map({
            _ =>
              val reply = Json.obj(
                "tableId" -> tableId,
                "columnId" -> columnId,
                "rowId" -> rowId
              )

              message.reply(reply)
          })

      case Some((tableId, columnId, None)) =>

        implicit val scalaCache = cache(tableId, columnId)

        removeAll()
          .map({
            _ =>
              val reply = Json.obj(
                "tableId" -> tableId,
                "columnId" -> columnId
              )

              message.reply(reply)
          })

      case None =>
        logger.error("Message invalid: Fields (tableId, columnId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, columnId) should be a Long")
    }
  }
}
