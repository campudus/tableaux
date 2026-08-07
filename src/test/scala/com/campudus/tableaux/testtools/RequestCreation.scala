package com.campudus.tableaux.testtools

import com.campudus.tableaux.database.domain.{Constraint, DefaultConstraint, DomainObject}
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.helper.Json

import io.vertx.lang.scala.json.{JsonArray, JsonObject}

object RequestCreation {

  case class Rows(columns: JsonArray, valueObjects: JsonObject*) extends DomainObject {

    import scala.jdk.CollectionConverters._

    val columnSeq = columns.asScala.toList
      .map(_.asInstanceOf[JsonObject])

    override def getJson: JsonObject = {
      val rowObjects = valueObjects
        .map(valueObject => {
          val valuesArray = columnSeq
            .map(column => {
              valueObject.getValue(column.getString("name"))
            })

          Json.obj("values" -> Json.arr(valuesArray*))
        })

      Json.obj(
        "columns" -> columns,
        "rows" -> Json.arr(rowObjects*)
      )
    }
  }

  object Columns {

    def apply(): Columns = {
      new Columns(Seq.empty*)
    }
  }

  case class Columns(columns: ColumnType*) extends DomainObject {

    def add(column: ColumnType): Columns = {
      Columns(columns.:+(column)*)
    }

    def getJson: JsonObject = Json.obj("columns" -> columns.map(_.getJson))
  }

  sealed abstract class ColumnType(val kind: String) extends DomainObject {
    val name: String

    def getJson: JsonObject = Json.obj("kind" -> kind, "name" -> name)
  }

  case class AttachmentCol(name: String) extends ColumnType("attachment")

  case class TextCol(name: String) extends ColumnType("text")

  case class ShortTextCol(name: String) extends ColumnType("shorttext")

  case class RichTextCol(name: String) extends ColumnType("richtext")

  case class NumericCol(override val name: String, decimalDigits: Option[Int] = None) extends ColumnType("numeric") {

    override def getJson: JsonObject = {
      super.getJson
        .mergeIn(
          decimalDigits match {
            case Some(digits) => Json.obj("decimalDigits" -> digits)
            case None => Json.obj()
          }
        )
    }
  }

  case class IntegerCol(name: String) extends ColumnType("integer")

  case class CurrencyCol(name: String) extends ColumnType("currency")

  case class BooleanCol(name: String) extends ColumnType("boolean")

  case class StatusCol(name: String) extends ColumnType("status")

  sealed abstract class BaseGroupCol(groups: Seq[ColumnId], showMemberColumns: Boolean) extends ColumnType("group") {

    override def getJson: JsonObject = {
      super.getJson.mergeIn(
        Json.obj(
          "groups" -> groups,
          "showMemberColumns" -> showMemberColumns
        )
      )
    }
  }

  case class GroupCol(name: String, groups: Seq[ColumnId], showMemberColumns: Boolean = false)
      extends BaseGroupCol(groups, showMemberColumns) {}

  sealed abstract class LinkCol extends ColumnType("link") {

    override val name: String
    val linkTo: TableId
    val biDirectional: Boolean

    override def getJson: JsonObject = {
      super.getJson.mergeIn(
        Json.obj(
          "toTable" -> linkTo,
          "singleDirection" -> !biDirectional
        )
      )
    }
  }

  case class LinkBiDirectionalCol(name: String, linkTo: TableId, constraint: Constraint = DefaultConstraint)
      extends LinkCol {

    override val biDirectional: Boolean = true

    override def getJson: JsonObject = super.getJson `mergeIn` Json.obj("constraint" -> constraint.getJson)
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
      column.getJson
        .mergeIn(
          Json.obj(
            "languageType" -> "country",
            "countryCodes" -> Json.arr(countryCodes*)
          )
        )
    }
  }

  case class Identifier(column: ColumnType) extends ColumnType(column.kind) {
    val name: String = column.name

    override def getJson: JsonObject = column.getJson.mergeIn(Json.obj("identifier" -> true))
  }

  case class FormattedGroupCol(
      name: String,
      groups: Seq[ColumnId],
      formatPattern: String,
      showMemberColumns: Boolean = false
  ) extends BaseGroupCol(groups, showMemberColumns) {

    override def getJson: JsonObject = {
      super.getJson
        .mergeIn(
          Json.obj(
            "formatPattern" -> formatPattern
          )
        )
    }
  }

  case class LinkAttributeDef(
      name: String,
      displayName: Map[String, String] = Map.empty,
      kind: String = "integer",
      multilanguage: Boolean = false
  ) extends DomainObject {

    override def getJson: JsonObject = Json.obj(
      "name" -> name,
      "displayName" -> Json.obj(displayName.toSeq*),
      "kind" -> kind,
      "multilanguage" -> multilanguage
    )
  }

  case class WithLinkAttributes(column: ColumnType, linkAttributes: Seq[LinkAttributeDef])
      extends ColumnType(column.kind) {
    val name: String = column.name

    override def getJson: JsonObject =
      column.getJson.mergeIn(Json.obj("linkAttributes" -> Json.arr(linkAttributes.map(_.getJson)*)))
  }

  case class WithFormatPattern(column: ColumnType, formatPattern: String) extends ColumnType(column.kind) {
    val name: String = column.name

    override def getJson: JsonObject = column.getJson.mergeIn(Json.obj("formatPattern" -> formatPattern))
  }

}
