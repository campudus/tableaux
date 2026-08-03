package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.helper.Json

import io.vertx.lang.scala.json._

case class Cell[A](column: ColumnType[A], rowId: RowId, value: A, rowLevelAnnotations: RowLevelAnnotations)
    extends DomainObject {

  override def getJson: JsonObject = {
    val finalFlagJson = rowLevelAnnotations.finalFlag match {
      case true => Json.obj("final" -> rowLevelAnnotations.finalFlag)
      case false => Json.obj()
    }

    val archivedFlagJson = rowLevelAnnotations.archivedFlag match {
      case true => Json.obj("archived" -> rowLevelAnnotations.archivedFlag)
      case false => Json.obj()
    }

    Json.obj("value" -> compatibilityGet(value)).mergeIn(finalFlagJson).mergeIn(archivedFlagJson)
  }
}
