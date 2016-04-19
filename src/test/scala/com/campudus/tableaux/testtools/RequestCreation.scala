package com.campudus.tableaux.testtools

import io.vertx.core.json.JsonObject
import org.vertx.scala.core.json.Json

object RequestCreation {

  object Columns {
    def apply(): Columns = {
      new Columns(Seq.empty)
    }
  }

  case class Columns(columns: Seq[ColumnType]) {
    def add(column: ColumnType): Columns = {
      Columns(columns.:+(column))
    }

    def getJson: JsonObject = Json.obj("columns" -> columns.map(_.getJson))
  }

  sealed abstract class ColumnType(val kind: String) {
    val name: String

    def getJson: JsonObject = Json.obj("kind" -> kind, "name" -> name)
  }

  case class AttachmentCol(name: String) extends ColumnType("attachment")

  case class TextCol(name: String) extends ColumnType("text")

  case class ShortTextCol(name: String) extends ColumnType("shorttext")

  case class RichTextCol(name: String) extends ColumnType("richtext")

  case class NumericCol(name: String) extends ColumnType("numeric")

  case class BooleanCol(name: String) extends ColumnType("boolean")

  case class Multilanguage(column: ColumnType) extends ColumnType(column.kind) {
    val name: String = column.name

    override def getJson: JsonObject = column.getJson.mergeIn(Json.obj("multilanguage" -> true))
  }

  case class Identifier(column: ColumnType) extends ColumnType(column.kind) {
    val name: String = column.name

    override def getJson: JsonObject = column.getJson.mergeIn(Json.obj("identifier" -> true))
  }

}