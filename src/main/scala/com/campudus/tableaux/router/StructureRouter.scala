package com.campudus.tableaux.router

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.StructureController
import com.campudus.tableaux.helper.JsonUtils._
import io.vertx.ext.web.RoutingContext
import org.vertx.scala.router.routing._

import scala.util.matching.Regex

object StructureRouter {
  def apply(config: TableauxConfig, controllerCurry: (TableauxConfig) => StructureController): StructureRouter = {
    new StructureRouter(config, controllerCurry(config))
  }
}

class StructureRouter(override val config: TableauxConfig, val controller: StructureController) extends BaseRouter {

  private val Column: Regex = "/tables/(\\d+)/columns/(\\d+)".r
  private val Columns: Regex = "/tables/(\\d+)/columns".r

  private val Table: Regex = "/tables/(\\d+)".r
  private val Tables: Regex = "/tables".r

  override def routes(implicit context: RoutingContext): Routing = {
    case Get(Tables()) => asyncGetReply(controller.retrieveTables())
    case Get(Table(tableId)) => asyncGetReply(controller.retrieveTable(tableId.toLong))

    /**
      * Get columns
      */
    case Get(Columns(tableId)) => asyncGetReply(controller.retrieveColumns(tableId.toLong))

    /**
      * Get columns
      */
    case Get(Column(tableId, columnId)) => asyncGetReply(controller.retrieveColumn(tableId.toLong, columnId.toLong))

    /**
      * Create Table
      */
    case Post(Tables()) => asyncSetReply {
      getJson(context) flatMap { json =>
        controller.createTable(json.getString("name"))
      }
    }

    /**
      * Create Column
      */
    case Post(Columns(tableId)) => asyncSetReply {
      getJson(context) flatMap (json => controller.createColumns(tableId.toLong, toCreateColumnSeq(json)))
    }

    /**
      * Change Table
      */
    case Post(Table(tableId)) => asyncEmptyReply {
      getJson(context) flatMap { json =>
        controller.changeTable(tableId.toLong, json.getString("name"))
      }
    }

    /**
      * Change Column
      */
    case Post(Column(tableId, columnId)) => asyncEmptyReply {
      getJson(context) flatMap {
        json =>
          val (optName, optOrd, optKind, optIdent) = toColumnChanges(json)
          controller.changeColumn(tableId.toLong, columnId.toLong, optName, optOrd, optKind, optIdent)
      }
    }

    /**
      * Delete Table
      */
    case Delete(Table(tableId)) => asyncEmptyReply(controller.deleteTable(tableId.toLong))

    /**
      * Delete Column
      */
    case Delete(Column(tableId, columnId)) => asyncEmptyReply(controller.deleteColumn(tableId.toLong, columnId.toLong))
  }
}