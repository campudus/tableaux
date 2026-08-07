package com.campudus.tableaux.database.domain

import com.campudus.tableaux.InvalidJsonException
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.helper.Json

import io.vertx.lang.scala.json._

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.joda.time.{DateTime, LocalDate}

case class Cardinality(from: Int, to: Int)

object DefaultCardinality extends Cardinality(0, 0)

case class Constraint(
    cardinality: Cardinality,
    deleteCascade: Boolean = false,
    archiveCascade: Boolean = false,
    finalCascade: Boolean = false
) extends DomainObject {

  override def getJson: JsonObject = {
    if (this == DefaultConstraint) {
      Json.obj()
    } else {
      Json.obj(
        "cardinality" -> Json.obj(
          "from" -> cardinality.from,
          "to" -> cardinality.to
        ),
        "deleteCascade" -> deleteCascade,
        "archiveCascade" -> archiveCascade,
        "finalCascade" -> finalCascade
      )
    }
  }
}

object DefaultConstraint extends Constraint(DefaultCardinality, false, false)

object LinkDirection {

  /**
    * Depending on the point of view, the link may be in different directions (left-to-right or right-to-left). This
    * method retrieves the correct link direction which can be used to get SQL and the correct table ids.
    *
    * @param fromTableId
    *   The table that was used to start from.
    * @param tableId1
    *   The table we found in the database in column 1.
    * @param tableId2
    *   The table we found in the database in column 2.
    * @return
    *   The correct link direction with the respective values.
    */
  def apply(
      fromTableId: TableId,
      tableId1: TableId,
      tableId2: TableId,
      cardinality1: Int,
      cardinality2: Int,
      deleteCascade: Boolean,
      archiveCascade: Boolean,
      finalCascade: Boolean
  ): LinkDirection = {

    // we need this because links can go both ways
    if (fromTableId == tableId1) {
      LeftToRight(
        tableId1,
        tableId2,
        Constraint(Cardinality(cardinality1, cardinality2), deleteCascade, archiveCascade, finalCascade)
      )
    } else {
      // no cascade functions in this direction
      RightToLeft(
        tableId2,
        tableId1,
        Constraint(
          Cardinality(cardinality2, cardinality1),
          deleteCascade = false,
          archiveCascade = false,
          finalCascade = false
        )
      )
    }
  }
}

/**
  * LinkColumn (created at Table A) points to Table B Retrieve link values for Table A => LeftToRight Retrieve link
  * values for Table B => RightToLeft More less it depends on the point of view.
  */
sealed trait LinkDirection {
  val from: TableId

  val to: TableId

  val constraint: Constraint

  def fromSql: String

  def toSql: String

  def orderingSql: String

  def fromCardinality: String

  def toCardinality: String

  def isManyToMany: Boolean = constraint.cardinality.from == 0 && constraint.cardinality.to == 0
}

case class LeftToRight(from: TableId, to: TableId, constraint: Constraint) extends LinkDirection {

  override def fromSql: String = "id_1"

  override def toSql: String = "id_2"

  override def orderingSql: String = "ordering_1"

  override def fromCardinality: String = {
    if (constraint.cardinality.from == 0) {
      Int.MaxValue.toString
    } else {
      "cardinality_1"
    }
  }

  override def toCardinality: String = {
    if (constraint.cardinality.to == 0) {
      Int.MaxValue.toString
    } else {
      "cardinality_2"
    }
  }
}

case class RightToLeft(from: TableId, to: TableId, constraint: Constraint) extends LinkDirection {

  override def fromSql: String = "id_2"

  override def toSql: String = "id_1"

  override def orderingSql: String = "ordering_2"

  override def fromCardinality: String = {
    if (constraint.cardinality.from == 0) {
      Int.MaxValue.toString
    } else {
      "cardinality_2"
    }
  }

  override def toCardinality: String = {
    if (constraint.cardinality.to == 0) {
      Int.MaxValue.toString
    } else {
      "cardinality_1"
    }
  }
}

/**
  * Definition of a single attribute that can be carried by a link (in addition to which rows it connects). Its
  * structure orients on ColumnDefinition, but it isn't a real column: it has no stable id (referenced by `name` only),
  * no ordering, and doesn't live in `system_columns` - it's stored as part of `system_link_table.attributes`.
  */
case class LinkAttributeDefinition(
    name: String,
    displayInfos: Seq[DisplayInfo],
    kind: TableauxDbType,
    multilanguage: Boolean
)

object LinkAttributeDefinition {

  val allowedKinds: Set[TableauxDbType] = Set(TextType, NumericType, IntegerType, BooleanType, DateType, DateTimeType)

  val maxCount = 1

  def fromJson(json: JsonObject): LinkAttributeDefinition = {
    LinkAttributeDefinition(
      name = json.getString("name"),
      displayInfos = DisplayInfos.fromJson(json),
      kind = TableauxDbType(json.getString("kind")),
      multilanguage = json.getBoolean("multilanguage", false)
    )
  }

  def seqFromJson(json: JsonArray): Seq[LinkAttributeDefinition] = {
    Option(json)
      .map(_.asScala.toSeq.collect({ case obj: JsonObject => fromJson(obj) }))
      .getOrElse(Seq.empty)
  }

  def getJson(attr: LinkAttributeDefinition): JsonObject = {
    val displayNameJson = attr.displayInfos.foldLeft(Json.obj()) {
      case (acc, displayInfo) =>
        displayInfo.optionalName
          .map(name => acc.mergeIn(Json.obj(displayInfo.langtag -> name)))
          .getOrElse(acc)
    }

    Json.obj(
      "name" -> attr.name,
      "displayName" -> displayNameJson,
      "kind" -> attr.kind.toString,
      "multilanguage" -> attr.multilanguage
    )
  }
}

/**
  * A row id to be linked, optionally carrying positional values for the target LinkColumn's `linkAttributes` (parallel
  * to the definitions array, exactly like the `attributes` array in a link cell's JSON value).
  */
case class LinkValue(id: RowId, attributes: Option[JsonArray] = None)

/**
  * Validates a link's attribute value array against its column's `linkAttributes` definitions. Implemented as small
  * standalone per-kind checks rather than constructing throwaway SimpleValueColumn instances, since those require a
  * full ColumnInformation/Table/RoleModel/TableauxUser context just to validate one scalar.
  */
object LinkAttributeValueValidator {

  def checkValidValue(definitions: Seq[LinkAttributeDefinition], attributes: JsonArray): Try[Unit] = Try {
    val values = Option(attributes).map(_.asScala.toSeq).getOrElse(Seq.empty)

    if (values.size != definitions.size) {
      throw InvalidJsonException(
        s"Expected ${definitions.size} link attribute value(s) but got ${values.size}.",
        "link-attributes"
      )
    }

    definitions.zip(values).foreach {
      case (definition, rawValue) =>
        if (definition.multilanguage) {
          rawValue match {
            case null => // no value set for this attribute, ok
            case obj: JsonObject =>
              obj.getMap.asScala.foreach({ case (_, langValue) => checkKindValue(definition, langValue) })
            case other =>
              throw InvalidJsonException(
                s"Attribute '${definition.name}' is multilanguage and expects an object of langtag to value, but got $other.",
                "link-attributes"
              )
          }
        } else {
          rawValue match {
            case null => // no value set for this attribute, ok
            case _: JsonObject =>
              throw InvalidJsonException(
                s"Attribute '${definition.name}' is not multilanguage and expects a single value, but got an object.",
                "link-attributes"
              )
            case value => checkKindValue(definition, value)
          }
        }
    }
  }

  private def checkKindValue(definition: LinkAttributeDefinition, value: Any): Unit = {
    val result: Try[Any] = definition.kind match {
      case TextType =>
        Try(value.asInstanceOf[String])
      case NumericType =>
        Try(value match {
          case n: Number => n
          case _ => throw new IllegalArgumentException(s"expected a number")
        })
      case IntegerType =>
        Try(value match {
          case i: Integer => i
          case _ => throw new IllegalArgumentException(s"expected an integer")
        })
      case BooleanType =>
        Try(value match {
          case b: Boolean => b
          case _ => throw new IllegalArgumentException(s"expected a boolean")
        })
      case DateType =>
        Try(LocalDate.parse(value.asInstanceOf[String]))
      case DateTimeType =>
        Try(DateTime.parse(value.asInstanceOf[String]))
      case other =>
        Failure(new IllegalArgumentException(s"unsupported link attribute kind: $other"))
    }

    result match {
      case Success(_) => ()
      case Failure(ex) =>
        throw InvalidJsonException(
          s"Invalid value for attribute '${definition.name}' (${definition.kind}): ${ex.getMessage}",
          "link-attributes"
        )
    }
  }
}
