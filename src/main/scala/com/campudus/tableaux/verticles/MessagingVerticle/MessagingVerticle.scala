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

import io.vertx.core.json.JsonObject
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.SQLConnection
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.Message
import io.vertx.scala.core.http.HttpClient
import io.vertx.scala.core.http.HttpServer
import io.vertx.scala.ext.web.client.WebClient
import org.vertx.scala.core.json.Json

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.typesafe.scalalogging.LazyLogging

object MessagingVerticle {
  val ADDRESS_CELL_CHANGED = "message.cell.change"
  val ADDRESS_SERVICES_CHANGE = "message.services.change"
  val ADDRESS_COLUMN_CREATED = "message.coluimns.created"

  val EVENT_TYPE_TABLE_CREATE = "table_create"
  val EVENT_TYPE_TABLE_CHANGE = "table_change"
  val EVENT_TYPE_TABLE_DELETE = "table_delete"
  val EVENT_TYPE_ROW_CREATED = "row_created"
  val EVENT_TYPE_ROW_DELETE = "row_deleted"
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
      val listeners = services.filter(service => service.serviceType.toString == ServiceType.LISTENER)
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
    eventBus.consumer(ADDRESS_CELL_CHANGED, messageHandlerCellChange).completionFuture()
    eventBus.consumer(ADDRESS_SERVICES_CHANGE, messageHandlerUpdateListeners).completionFuture()
    eventBus.consumer(ADDRESS_COLUMN_CREATED, messageHandlerColumnCreated).completionFuture()
  }

  private def messageHandlerUpdateListeners(message: Message[JsonObject]): Unit = {
    for {
      listenersMap <- retrieveListeners()
    } yield {
      listeners = listenersMap
      message.reply("")
    }
  }

  private def messageHandlerCellChange(message: Message[JsonObject]): Unit = {
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
    } yield {
      println(dependentCellValues.head)
      val data = Json.obj("cell" -> cell.getJson, "dependentCells" -> dependentCellValues)
      val listeners = getApplicableListeners(EVENT_TYPE_CELL_CHANGED, Some(table.getJson), Some(column.getJson))
      println(listeners)
      sendMessage(listeners, createPayload(EVENT_TYPE_CELL_CHANGED, Some(tableId), Some(columnId), Some(rowId), data))

      message.reply("")
    }
  }

  private def sendMessage(listeners: Seq[Service], payLoad: JsonObject): Unit = {
    listeners.foreach(listener => {
      val listenerData: Try[(Int, String, String, JsonObject)] = Try({
        val config = listener.config
        val port = config.getInteger("port")
        val host = config.getString("host")
        val route = config.getString("route")
        val headers = config.getJsonObject("headers", Json.obj())
        (port, host, route, headers)
      })

      listenerData match {
        case Success((port, host, route, headers)) => {
          webClient.post(port, host, route).sendJsonObject(payLoad, res => println("ok"))
        }
        case Failure(e) => {
          println(e)
        }
      }

    })
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

  private def messageHandlerColumnCreated(message: Message[JsonObject]): Unit = {
    val body = message.body()
    val tableId = body.getLong("tableId").asInstanceOf[TableId]
    val columnId = body.getLong("columnId")
    for {
      table <- tableauxModel.retrieveTable(tableId, true)
      column <- tableauxModel.retrieveColumn(table, columnId)
    } yield {
      val listeners = getApplicableListeners(EVENT_TYPE_COLUMN_CREATE, Some(table.getJson), Some(column.getJson))
      val payload = createPayload(EVENT_TYPE_COLUMN_CREATE, Some(tableId), Some(columnId), None, column.getJson)
      sendMessage(listeners, payload)
    }

  }
}
