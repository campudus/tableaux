package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._

import org.vertx.scala.core.json._

case class Cell[A](column: ColumnType[A], rowId: RowId, value: A, rowLevelAnnotations: RowLevelAnnotations)
    extends DomainObject {

  override def getJson: JsonObject = {
    val finalFlagJson = rowLevelAnnotations.finalFlag match {
      case true => Json.obj("final" -> rowLevelAnnotations.finalFlag)
      case false => Json.emptyObj()
    }

    val archivedFlagJson = rowLevelAnnotations.archivedFlag match {
      case true => Json.obj("archived" -> rowLevelAnnotations.archivedFlag)
      case false => Json.emptyObj()
    }

    Json.obj("value" -> compatibilityGet(value)).mergeIn(finalFlagJson).mergeIn(archivedFlagJson)
  }
}
