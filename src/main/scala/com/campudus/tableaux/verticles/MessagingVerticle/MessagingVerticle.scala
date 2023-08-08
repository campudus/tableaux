package com.campudus.tableaux.verticles.Messaging

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.ColumnType
import com.campudus.tableaux.database.domain.Service
import com.campudus.tableaux.database.domain.ServiceType
import com.campudus.tableaux.database.domain.Table
import com.campudus.tableaux.database.model.ServiceModel
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.model.TableauxModel
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.campudus.tableaux.router.auth.permission.TableauxUser

import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.SQLConnection
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.Message
import io.vertx.scala.core.http.HttpClient
import io.vertx.scala.core.http.HttpServer
import io.vertx.scala.ext.web.client.HttpResponse
import io.vertx.scala.ext.web.client.WebClient
import org.vertx.scala.core.json.Json

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.typesafe.scalalogging.LazyLogging

object MessagingVerticle {
  val KEY_TABLE_ID = "tableId"
  val KEY_COLUMN_ID = "columnId"
  val KEY_ROW_ID = "rowId"
  val ID_KEYS = Seq(KEY_TABLE_ID, KEY_COLUMN_ID, KEY_ROW_ID)

  val ADDRESS_CELL_CHANGED = "message.cell.change"
  val ADDRESS_SERVICES_CHANGE = "message.services.change"
  val ADDRESS_COLUMN_CREATED = "message.columns.created"
  val ADDRESS_COLUMN_CHANGED = "message.columns.changed"
  val ADDRESS_COLUMN_DELETED = "message.columns.deleted"

  val ADDRESS_TABLE_CREATED = "message.tables.created"
  val ADDRESS_TABLE_CHANGED = "message.tables.changed"
  val ADDRESS_TABLE_DELETED = "message.tables.deleted"

  val ADDRESS_ROW_DELETED = "message.rows.deleted"
  val ADDRESS_ROW_CREATED = "message.rows.created"
  val ADDRESS_ROW_ANNOTATION_CHANGED = "message.rows.annotation.changed"

  val ADDRESS_CELL_ANNOTATION_CHANGED = "message.cells.annotation.changed"

  val EVENT_TYPE_TABLE_CREATE = "table_create"
  val EVENT_TYPE_TABLE_CHANGE = "table_change"
  val EVENT_TYPE_TABLE_DELETE = "table_delete"
  val EVENT_TYPE_ROW_CREATED = "row_create"
  val EVENT_TYPE_ROW_DELETE = "row_delete"
  val EVENT_TYPE_ROW_ANNOTATION_CHANGED = "row_annotation_changed"
  val EVENT_TYPE_COLUMN_CREATE = "column_create"
  val EVENT_TYPE_COLUMN_CHANGE = "column_change"
  val EVENT_TYPE_COLUMN_DELETE = "column_delete"
  val EVENT_TYPE_CELL_ANNOTATION_CHANGED = "cell_annotation_changed"
  val EVENT_TYPE_CELL_CHANGED = "cell_changed"

  val eventTypes = Seq(
    EVENT_TYPE_TABLE_CREATE,
    EVENT_TYPE_TABLE_CHANGE,
    EVENT_TYPE_TABLE_DELETE,
    EVENT_TYPE_ROW_CREATED,
    EVENT_TYPE_ROW_DELETE,
    EVENT_TYPE_ROW_ANNOTATION_CHANGED,
    EVENT_TYPE_COLUMN_CREATE,
    EVENT_TYPE_COLUMN_CHANGE,
    EVENT_TYPE_COLUMN_DELETE,
    EVENT_TYPE_CELL_CHANGED,
    EVENT_TYPE_CELL_ANNOTATION_CHANGED
  )
}

class MessagingVerticle extends ScalaVerticle
    with LazyLogging {

  import MessagingVerticle._

  private var tableauxModel: TableauxModel = _
  private var serviceModel: ServiceModel = _
  private var structureModel: StructureModel = _

  private var listeners: Map[String, Seq[Service]] = _

  private implicit var user: TableauxUser = _

  private lazy val eventBus = vertx.eventBus()

  private lazy val webClient: WebClient = WebClient.create(vertx)

  private def retrieveListeners(): Future[Map[String, Seq[Service]]] = {
    for {
      services <- serviceModel.retrieveAll()
    } yield {
      val listeners = services.filter(service => {

        val hasRequiredConfigValues =
          Try({
            val config = service.config
            val port = config.getInteger("port")
            val host = config.getString("host")
            val route = config.getString("route")
            val headers = config.getJsonObject("headers", Json.obj())
            (port, host, route, headers)
          }).isSuccess

        service.serviceType.toString == ServiceType.LISTENER && hasRequiredConfigValues
      })
      val listenerMap = eventTypes.foldLeft[Map[String, Seq[Service]]](Map()) { (acc, eventType) =>
        {
          val listenersWithEventType = listeners.filter(service =>
            service.config.getJsonArray("events", Json.arr()).asScala.toSeq.contains(eventType)
          )
          acc + (eventType -> listenersWithEventType)
        }
      }
      listenerMap
    }
  }

  private def getApplicableListeners(
      eventType: String,
      maybeTableJson: Option[JsonObject] = None,
      maybeColumnJson: Option[JsonObject]
  ): Seq[Service] = {
    val listenersForEventType = listeners getOrElse (eventType, Seq())
    val getFilterRules: (String, String, JsonObject) => Seq[JsonObject] =
      (objectType: String, ruleType: String, scope: JsonObject) => {
        scope.getJsonObject(objectType, Json.obj()).getJsonArray(ruleType, Json.arr()).asScala.toSeq.map(obj =>
          obj.asInstanceOf[JsonObject]
        )
      }

    val filterFunction: (Service) => Boolean = (service: Service) => {
      val scope = service.scope

      val applyRules = (rules: Seq[JsonObject], objectJson: JsonObject) => {
        rules.exists(rule =>
          rule.fieldNames().asScala.toSeq.forall(key => {
            val str = rule.getValue(key, "").toString
            // If the first character in the regular expression is a + or *, don't parse regex
            // as this would result in an endless loop
            Seq("+", "*").contains(str.substring(0, 1)) match {
              case true => false
              case false => {
                val regex = str.r
                objectJson.getValue(key, "").toString match {
                  case regex() => true
                  case _ => false
                }

              }
            }
          })
        )
      }
      maybeTableJson match {
        case Some(table) => {
          val tableIncludes = getFilterRules("tables", "includes", scope)
          val tableExcludes = getFilterRules("tables", "excludes", scope)
          val shouldIncludeTable = applyRules(tableIncludes, table) && !applyRules(tableExcludes, table)
          maybeColumnJson match {
            case Some(column) => {
              val columnIncludes = getFilterRules("columns", "includes", scope)
              val columnExcludes = getFilterRules("columns", "excludes", scope)
              val colIn = applyRules(columnIncludes, column)
              val colEx = !applyRules(columnExcludes, column)

              shouldIncludeTable && colIn && colEx
            }
            case None => {
              shouldIncludeTable
            }
          }
        }
        case None => true
      }

    }

    val filtered = listenersForEventType.filter(filterFunction)
    filtered

  }

  override def startFuture(): Future[_] = {
    logger.info("start future")
    val isAuthorization: Boolean = !config.getJsonObject("authConfig").isEmpty
    val roles = config.getJsonObject("rolePermissions")
    implicit val roleModel: RoleModel = RoleModel(roles, isAuthorization)
    val vertxAccess = new VertxAccess { override val vertx: Vertx = MessagingVerticle.this.vertx }
    val connection = SQLConnection(vertxAccess, config.getJsonObject("databaseConfig"))
    val dbConnection = DatabaseConnection(vertxAccess, connection)
    structureModel = StructureModel(dbConnection)
    user = TableauxUser("", roles.fieldNames().asScala.toSeq)
    tableauxModel = TableauxModel(dbConnection, structureModel)
    serviceModel = ServiceModel(dbConnection)
    for {
      listenersMap <- retrieveListeners()
      _ <- registerConsumers()
    } yield {
      listeners = listenersMap
    }

  }

  private def registerConsumers(): Future[Unit] = {
    eventBus.consumer(ADDRESS_CELL_CHANGED, errorHandler(messageHandlerCellChange)).completionFuture()
    eventBus.consumer(ADDRESS_SERVICES_CHANGE, messageHandlerUpdateListeners).completionFuture()
    eventBus.consumer(ADDRESS_COLUMN_CREATED, errorHandler(messageHandlerColumnCreated())).completionFuture()
    eventBus.consumer(ADDRESS_COLUMN_CHANGED, errorHandler(messageHandlerColumnChanged)).completionFuture()
    eventBus.consumer(ADDRESS_COLUMN_DELETED, errorHandler(messageHandlerColumnDeleted)).completionFuture()
    eventBus.consumer(ADDRESS_TABLE_CREATED, errorHandler(messageHandlerTableCreated())).completionFuture()
    eventBus.consumer(ADDRESS_TABLE_CHANGED, errorHandler(messageHandlerTableChanged)).completionFuture()
    eventBus.consumer(ADDRESS_TABLE_DELETED, errorHandler(messageHandlerTableDeleted)).completionFuture()
    eventBus.consumer(ADDRESS_ROW_CREATED, errorHandler(messageHandlerRowCreated)).completionFuture()
    eventBus.consumer(ADDRESS_ROW_DELETED, errorHandler(messageHandlerRowDeleted)).completionFuture()
    eventBus.consumer(
      ADDRESS_ROW_ANNOTATION_CHANGED,
      errorHandler(messageHandlerRowAnnotationChanged)
    ).completionFuture()
    eventBus.consumer(
      ADDRESS_CELL_ANNOTATION_CHANGED,
      errorHandler(messageHandlerCellAnnotationChanged)
    ).completionFuture()
  }

  private def messageHandlerUpdateListeners(message: Message[JsonObject]): Unit = {
    for {
      listenersMap <- retrieveListeners()
    } yield {
      listeners = listenersMap
      message.reply("ok")
    }
  }

  private def messageHandlerCellChange(message: Message[JsonObject]): Future[Seq[Any]] = {
    implicit val user: TableauxUser = TableauxUser("Messaging Verticle", Seq("dev"))
    val body = message.body()
    val tableId = body.getLong("tableId").asInstanceOf[TableId]
    val columnId = body.getLong("columnId").asInstanceOf[ColumnId]
    val rowId = body.getLong("rowId").asInstanceOf[RowId]
    for {
      table <- tableauxModel.retrieveTable(tableId, true)
      column <- tableauxModel.retrieveColumn(table, columnId)
      cell <- tableauxModel.retrieveCell(table, column.id, rowId)
      dependentCells <- tableauxModel.retrieveDependentCells(table, rowId)
      dependentCellValues <- Future.sequence(dependentCells.map({
        case (table, linkColumn, rowIds) => {
          rowIds.map(rowId =>
            tableauxModel.retrieveCell(table, linkColumn.id, rowId, false).map(data =>
              Json.obj("table" -> table.id, "column" -> linkColumn.id, "row" -> rowId).mergeIn(data.getJson)
            )
          )
        }
      }).flatten)
      data = Json.obj("cell" -> cell.getJson, "dependentCells" -> dependentCellValues)
      listeners = getApplicableListeners(EVENT_TYPE_CELL_CHANGED, Some(table.getJson), Some(column.getJson))
      listenerResponses <-
        sendMessage(listeners, createPayload(EVENT_TYPE_CELL_CHANGED, Some(tableId), Some(columnId), Some(rowId), data))
    } yield {
      listenerResponses
    }
  }

  def sendMessage(
      listeners: Seq[Service],
      payLoad: JsonObject
  ): Future[Seq[Any]] = {
    Future.sequence(listeners.map(listener => {
      val name = listener.name
      val config = listener.config
      val port = config.getInteger("port")
      val host = config.getString("host")
      val route = config.getString("route")
      val headers = config.getJsonObject("headers", Json.obj())

      webClient.post(port, host, route).sendJsonObjectFuture(payLoad).recover { case err: Throwable =>
        logger.error(s"Service Name: $name, Reason: ${err.getMessage}")
      }
    }))
  }

  private def createPayload(
      event: String,
      tableId: Option[TableId] = None,
      columnId: Option[ColumnId] = None,
      rowId: Option[RowId] = None,
      data: JsonObject = Json.obj()
  ): JsonObject = {
    var baseObj = Json.obj("event" -> event, "data" -> data)
    tableId match {
      case Some(id) => baseObj.put("tableId", id)
      case _ => {}
    }
    columnId match {
      case Some(id) => baseObj.put("columnId", id)
      case _ => {}
    }
    rowId match {
      case Some(id) => baseObj.put("rowId", id)
      case _ => {}
    }
    baseObj
  }

  private def messageHandlerColumnCreated(
      eventType: String = EVENT_TYPE_COLUMN_CREATE
  )(
      message: Message[JsonObject]
  ): Future[Seq[Any]] = {
    val body = message.body()
    val tableId = body.getLong("tableId").asInstanceOf[TableId]
    val columnId = body.getLong("columnId")
    for {
      table <- tableauxModel.retrieveTable(tableId, true)
      column <- tableauxModel.retrieveColumn(table, columnId)
      listeners = getApplicableListeners(eventType, Some(table.getJson), Some(column.getJson))
      payload = createPayload(eventType, Some(tableId), Some(columnId), None, column.getJson)
      listenerResponses <- sendMessage(listeners, payload)
    } yield {
      listenerResponses
    }

  }

  private def messageHandlerTableCreated(
      eventType: String = EVENT_TYPE_TABLE_CREATE
  )(
      message: Message[JsonObject]
  ): Future[Seq[Any]] = {
    val body = message.body()
    val tableId = body.getLong("tableId").asInstanceOf[TableId]
    for {
      table <- tableauxModel.retrieveTable(tableId, true)
      listeners = getApplicableListeners(eventType, Some(table.getJson), None)
      payload = createPayload(eventType, Some(tableId), None, None, table.getJson)
      listenerResponses <- sendMessage(listeners, payload)
    } yield {
      listenerResponses
    }

  }

  private def messageHandlerTableChanged(message: Message[JsonObject]): Future[Seq[Any]] = {
    messageHandlerTableCreated(EVENT_TYPE_TABLE_CHANGE)(message)
  }

  private def messageHandlerColumnChanged(message: Message[JsonObject]): Future[Seq[Any]] = {
    messageHandlerColumnCreated(EVENT_TYPE_COLUMN_CHANGE)(message)
  }

  private def getJsonValueAsOption[A](key: String, obj: JsonObject): Option[A] = {
    Try(obj.getValue(key).asInstanceOf[A]) match {
      // getValue returns null if the key doesn't exist ....
      case Success(null) => None
      case Success(value) => Some(value)
      case Failure(_) => None
    }
  }

  private def getId(key: String, obj: JsonObject): Option[Long] = getJsonValueAsOption(key, obj)

  private def getIds(body: JsonObject): (Option[Long], Option[Long], Option[Long]) = {
    ID_KEYS.map(key => getId(key, body)) match {
      case Seq(a, b, c) => (a, b, c)
    }
  }

  private def messageHandlerTableDeleted(message: Message[JsonObject]): Future[Seq[Any]] = {
    val body = message.body()
    val (tableId, columnId, rowId) = getIds(body)
    val deletedTable = body.getJsonObject("table", Json.obj())
    val listeners = getApplicableListeners(EVENT_TYPE_TABLE_DELETE, Some(deletedTable), None)
    sendMessage(listeners, createPayload(EVENT_TYPE_TABLE_DELETE, tableId, columnId, rowId, deletedTable))
  }

  private def messageHandlerColumnDeleted(message: Message[JsonObject]): Future[Seq[Any]] = {
    val body = message.body()
    val (tableId, columnId, rowId) = getIds(body)
    val deletedColumn = body.getJsonObject("column", Json.obj())
    for {
      table <- tableauxModel.retrieveTable(tableId.get, true)
      listeners = getApplicableListeners(EVENT_TYPE_COLUMN_DELETE, Some(table.getJson), Some(deletedColumn))
      listenerResponses <-
        sendMessage(listeners, createPayload(EVENT_TYPE_COLUMN_DELETE, tableId, columnId, rowId, deletedColumn))
    } yield { listenerResponses }
  }

  private def messageHandlerRowDeleted(message: Message[JsonObject]): Future[Seq[Any]] = {
    val body = message.body()
    val (tableId, columnId, rowId) = getIds(body)
    for {
      table <- tableauxModel.retrieveTable(tableId.get, true)
      listeners = getApplicableListeners(EVENT_TYPE_ROW_DELETE, Some(table.getJson), None)
      listenerResponses <- sendMessage(listeners, createPayload(EVENT_TYPE_ROW_DELETE, tableId, columnId, rowId))
    } yield {
      listenerResponses
    }
  }

  private def messageHandlerRowCreated(message: Message[JsonObject]): Future[Seq[Any]] = {
    val body = message.body()
    val (tableId, columnId, rowId) = getIds(body)
    for {
      table <- tableauxModel.retrieveTable(tableId.get, true)
      row <- tableauxModel.retrieveRow(table, rowId.get)
      listeners = getApplicableListeners(EVENT_TYPE_ROW_CREATED, Some(table.getJson), None)
      listenerResponses <-
        sendMessage(listeners, createPayload(EVENT_TYPE_ROW_CREATED, tableId, columnId, rowId, row.getJson))
    } yield {
      listenerResponses
    }
  }

  private def messageHandlerCellAnnotationChanged(message: Message[JsonObject]): Future[Seq[Any]] = {
    val body = message.body()
    val (tableId, columnId, rowId) = getIds(body)
    for {
      table <- tableauxModel.retrieveTable(tableId.get, true)
      column <- tableauxModel.retrieveColumn(table, columnId.get)
      cellAnnotations <- tableauxModel.retrieveCellAnnotations(table, columnId.get, rowId.get)
      listeners =
        getApplicableListeners(EVENT_TYPE_CELL_ANNOTATION_CHANGED, Some(table.getJson), Some(column.getJson))
      listenerResponses <- sendMessage(
        listeners,
        createPayload(EVENT_TYPE_CELL_ANNOTATION_CHANGED, tableId, columnId, rowId, cellAnnotations.getJson)
      )
    } yield {
      listenerResponses
    }
  }

  private def messageHandlerRowAnnotationChanged(message: Message[JsonObject]): Future[Seq[Any]] = {
    val body = message.body()
    val (tableId, columnId, rowId) = getIds(body)
    for {
      table <- tableauxModel.retrieveTable(tableId.get, true)
      row <- tableauxModel.retrieveRow(table, rowId.get)
      listeners =
        getApplicableListeners(EVENT_TYPE_ROW_ANNOTATION_CHANGED, Some(table.getJson), None)
      listenerResponses <- sendMessage(
        listeners,
        createPayload(EVENT_TYPE_ROW_ANNOTATION_CHANGED, tableId, columnId, rowId, row.getJson)
      )
    } yield {
      listenerResponses
    }
  }

  private def errorHandler(
      messageHandler: Message[JsonObject] => Future[Seq[Any]]
  )(message: Message[JsonObject]): Unit = {

    messageHandler(message).onComplete {
      case Success(_) => {
        message.reply("ok")
      }
      case Failure(exception) => {
        logger.error(exception.getMessage)
        message.fail(500, exception.getMessage)
      }
    }
  }
}
