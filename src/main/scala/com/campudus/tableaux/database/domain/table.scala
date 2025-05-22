package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.router.auth.permission.{ComparisonObjects, RoleModel, TableauxUser}

import io.vertx.core.json.JsonObject
import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.core.json._

object TableType {

  def apply(string: String): TableType = {
    string match {
      case GenericTable.NAME => GenericTable
      case SettingsTable.NAME => SettingsTable
      case TaxonomyTable.NAME => TaxonomyTable
      case UnionTable.NAME => UnionTable
      case _ => throw new IllegalArgumentException("Unknown table type")
    }
  }
}

sealed trait TableType {
  val NAME: String
}

case object GenericTable extends TableType {
  override val NAME = "generic"
}

case object SettingsTable extends TableType {
  override val NAME = "settings"
}

case object TaxonomyTable extends TableType {
  override val NAME = "taxonomy"
}

case object UnionTable extends TableType {
  override val NAME = "union"
}

object Table {

  def apply(id: TableId)(implicit roleModel: RoleModel, user: TableauxUser): Table = {
    Table(id, "unknown", hidden = false, None, Seq.empty, GenericTable, None, None, None)
  }
}

case class Table(
    id: TableId,
    name: String,
    hidden: Boolean,
    langtags: Option[Seq[String]],
    displayInfos: Seq[DisplayInfo],
    tableType: TableType,
    tableGroup: Option[TableGroup],
    attributes: Option[JsonObject],
    concatFormatPattern: Option[String]
)(implicit roleModel: RoleModel = RoleModel(), user: TableauxUser) extends DomainObject {

  override def getJson: JsonObject = {
    val tableJson = Json.obj(
      "id" -> id,
      "name" -> name,
      "hidden" -> hidden,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "attributes" -> attributes.getOrElse(new JsonObject("{}"))
    )

    langtags.foreach(lt => tableJson.mergeIn(Json.obj("langtags" -> lt)))

    displayInfos.foreach { di =>
      {
        di.optionalName.map(name => {
          tableJson.mergeIn(
            Json.obj("displayName" -> tableJson.getJsonObject("displayName").mergeIn(Json.obj(di.langtag -> name)))
          )
        })
        di.optionalDescription.map(desc => {
          tableJson.mergeIn(
            Json.obj("description" -> tableJson.getJsonObject("description").mergeIn(Json.obj(di.langtag -> desc)))
          )
        })
      }
    }

    if (tableType != GenericTable) {
      tableJson.mergeIn(Json.obj("type" -> tableType.NAME))
    }

    val concatFormatPatternJson = concatFormatPattern match {
      case Some(pattern) => Json.obj("concatFormatPattern" -> pattern)
      case None => Json.obj()
    }

    tableJson.mergeIn(concatFormatPatternJson)

    tableGroup.foreach(tg => tableJson.put("group", tg.getJson))

    tableJson
  }
}

case class TableSeq(tables: Seq[Table])(
    implicit roleModel: RoleModel = RoleModel(),
    user: TableauxUser
) extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj("tables" -> compatibilityGet(tables))
  }
}

case class CompleteTable(table: Table, columns: Seq[ColumnType[_]], rowList: RowSeq)(implicit user: TableauxUser)
    extends DomainObject {

  override def getJson: JsonObject = {
    table.getJson
      .mergeIn(Json.obj("columns" -> columns.map {
        _.getJson
      }))
      .mergeIn(rowList.getJson)
  }
}

case class TablesStructure(tables: Seq[Table], columnMap: Map[TableId, Seq[ColumnType[_]]])(implicit user: TableauxUser)
    extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj("tables" -> tables.map(tbl => {
      tbl.getJson.mergeIn(
        Json.obj("columns" -> {
          val columns = columnMap.getOrElse(tbl.id, Seq[ColumnType[_]]())
          columns.map(_.getJson)
        })
      )
    }))
  }
}
