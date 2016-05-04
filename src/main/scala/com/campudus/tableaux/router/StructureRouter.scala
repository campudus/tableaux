package com.campudus.tableaux.router

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.StructureController
import com.campudus.tableaux.helper.JsonUtils._
import io.vertx.ext.web.RoutingContext
import org.vertx.scala.router.routing._

import scala.collection.JavaConverters._
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
  private val TableOrder: Regex = "/tables/(\\d+)/order".r

  override def routes(implicit context: RoutingContext): Routing = {

    /**
      * Get tables
      */
    case Get(Tables()) => asyncGetReply {
      controller.retrieveTables()
    }

    /**
      * Get table
      */
    case Get(Table(tableId)) => asyncGetReply {
      controller.retrieveTable(tableId.toLong)
    }

    /**
      * Get columns
      */
    case Get(Columns(tableId)) => asyncGetReply {
      controller.retrieveColumns(tableId.toLong)
    }

    /**
      * Get columns
      */
    case Get(Column(tableId, columnId)) => asyncGetReply {
      controller.retrieveColumn(tableId.toLong, columnId.toLong)
    }

    /**
      * Create Table
      */
    case Post(Tables()) => asyncGetReply {
      for {
        json <- getJson(context)

        name = json.getString("name")
        hidden = json.getBoolean("hidden", false).booleanValue()

        // if contains than user wants langtags to be set
        // but then langtags could be null so that's the second option
        //
        // langtags == null => global langtags
        // langtags == [] => [] table without langtags
        // langtags == ['de-DE', 'en-GB'] => table with two langtags
        //
        // {} => None => db: null
        // {langtags:null} => Some(None) => db: null
        // {langtags:['de-DE']} => Some(Seq('de-DE')) => db: ['de-DE']
        langtags = booleanToValueOption(json.containsKey("langtags"), Option(json.getJsonArray("langtags")).map(_.asScala.map(_.toString).toSeq))

        created <- controller.createTable(name, hidden, langtags)
      } yield created
    }

    /**
      * Create Column
      */
    case Post(Columns(tableId)) => asyncGetReply {
      for {
        json <- getJson(context)
        created <- controller.createColumns(tableId.toLong, toCreateColumnSeq(json))
      } yield created
    }

    /**
      * Change Table
      */
    case Post(Table(tableId)) => asyncGetReply {
      for {
        json <- getJson(context)

        name = Option(json.getString("name"))
        hidden = Option(json.getBoolean("hidden")).map(_.booleanValue())

        // if contains than user wants langtags to be set
        // but then langtags could be null so that's the second option
        //
        // langtags == null => global langtags
        // langtags == [] => [] table without langtags
        // langtags == ['de-DE', 'en-GB'] => table with two langtags
        //
        // {} => None => db: do nothing
        // {langtags:null} => Some(None) => db: overwrite with null
        // {langtags:['de-DE']} => Some(Seq('de-DE')) => db: overwrite with ['de-DE']
        langtags = booleanToValueOption(json.containsKey("langtags"), Option(json.getJsonArray("langtags")).map(_.asScala.map(_.toString).toSeq))

        updated <- controller.changeTable(tableId.toLong, name, hidden, langtags)
      } yield updated
    }

    /**
      * Change Table ordering
      */
    case Post(TableOrder(tableId)) => asyncEmptyReply {
      for {
        json <- getJson(context)
        result <- controller.changeTableOrder(tableId.toLong, json.getString("location"), Option(json.getLong("id")).map(_.toLong))
      } yield result
    }

    /**
      * Change Column
      */
    case Post(Column(tableId, columnId)) => asyncGetReply {
      for {
        json <- getJson(context)
        (optName, optOrd, optKind, optIdent, optDisplayNames, optDescription) = toColumnChanges(json)
        changed <- controller.changeColumn(tableId.toLong, columnId.toLong, optName, optOrd, optKind, optIdent, optDisplayNames, optDescription)
      } yield changed
    }

    /**
      * Delete Table
      */
    case Delete(Table(tableId)) => asyncEmptyReply {
      controller.deleteTable(tableId.toLong)
    }

    /**
      * Delete Column
      */
    case Delete(Column(tableId, columnId)) => asyncEmptyReply {
      controller.deleteColumn(tableId.toLong, columnId.toLong)
    }
  }

}
