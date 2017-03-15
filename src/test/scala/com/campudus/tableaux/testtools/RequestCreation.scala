package com.campudus.tableaux.testtools

import com.campudus.tableaux.database.model.TableauxModel.TableId
import org.vertx.scala.core.json.JsonObject
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

  case class CurrencyCol(name: String) extends ColumnType("currency")

  case class BooleanCol(name: String) extends ColumnType("boolean")

  sealed abstract class LinkCol extends ColumnType("link") {

    override val name: String
    val linkTo: TableId
    val biDirectional: Boolean

    override def getJson: JsonObject = {
      super.getJson.mergeIn(Json.obj(
        "toTable" -> linkTo,
        "singleDirection" -> !biDirectional
      ))
    }
  }

  case class LinkBiDirectionalCol(name: String, linkTo: TableId) extends LinkCol {

    override val biDirectional: Boolean = true
  }

  case class LinkUniDirectionalCol(name: String, linkTo: TableId) extends LinkCol {

    override val biDirectional: Boolean = false
  }

  case class Multilanguage(column: ColumnType) extends ColumnType(column.kind) {
    val name: String = column.name

    override def getJson: JsonObject = column.getJson.mergeIn(Json.obj("multilanguage" -> true))
  }

  case class MultiCountry(column: ColumnType, countryCodes: Seq[String]) extends ColumnType(column.kind) {

    val name: String = column.name

    override def getJson: JsonObject = {
      column
        .getJson
        .mergeIn(
          Json.obj(
            "languageType" -> "country",
            "countryCodes" -> Json.arr(countryCodes: _*)
          )
        )
    }
  }

  case class Identifier(column: ColumnType) extends ColumnType(column.kind) {
    val name: String = column.name

    override def getJson: JsonObject = column.getJson.mergeIn(Json.obj("identifier" -> true))
  }

}