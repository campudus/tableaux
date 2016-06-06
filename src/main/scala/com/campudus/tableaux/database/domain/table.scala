package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

case class Table(id: TableId, name: String, hidden: Boolean, langtags: Option[Seq[String]], displayInfos: Seq[DisplayInfo]) extends DomainObject {
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
