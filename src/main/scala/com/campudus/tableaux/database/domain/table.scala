package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

object TableType {
  def apply(string: String): TableType = {
    string match {
      case GenericTable.NAME => GenericTable
      case SettingsTable.NAME => SettingsTable
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

case class Table(id: TableId, name: String, hidden: Boolean, langtags: Option[Seq[String]], displayInfos: Seq[DisplayInfo], tableType: TableType, tableGroup: Option[TableGroup]) extends DomainObject {
  override def getJson: JsonObject = {
    val result = Json.obj(
      "id" -> id,
      "name" -> name,
      "hidden" -> hidden,
      "displayName" -> Json.obj(),
      "description" -> Json.obj()
    )

    if (langtags.isDefined) {
      result.mergeIn(Json.obj("langtags" -> langtags.orNull))
    }

    displayInfos.foreach { di =>
      di.optionalName.map(name => result.mergeIn(Json.obj("displayName" -> result.getJsonObject("displayName").mergeIn(Json.obj(di.langtag -> name)))))
      di.optionalDescription.map(desc => result.mergeIn(Json.obj("description" -> result.getJsonObject("description").mergeIn(Json.obj(di.langtag -> desc)))))
    }

    if (tableType != GenericTable) {
      result.mergeIn(Json.obj("type" -> tableType.NAME))
    }

    if (tableGroup.isDefined) {
      result.put("group", tableGroup.get.getJson)
    }

    result
  }
}

case class TableSeq(tables: Seq[Table]) extends DomainObject {
  override def getJson: JsonObject = Json.obj("tables" -> compatibilityGet(tables))
}

case class CompleteTable(table: Table, columns: Seq[ColumnType[_]], rowList: RowSeq) extends DomainObject {
  override def getJson: JsonObject = table.getJson.mergeIn(Json.obj("columns" -> columns.map {
    _.getJson
  })).mergeIn(rowList.getJson)
}
