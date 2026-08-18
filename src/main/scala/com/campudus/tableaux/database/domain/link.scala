package com.campudus.tableaux.database.domain

import com.campudus.tableaux.{InvalidJsonException, UnprocessableEntityException}
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.helper.Json

import io.vertx.lang.scala.json._

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.joda.time.{DateTime, DateTimeZone, LocalDate}

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
    // Both halves of a DisplayInfo have to be written out, exactly like ColumnType.getJson does it: this JSON is
    // not just the API response, it is also what gets persisted to system_link_table.attributes - so anything
    // dropped here is dropped for good, not merely hidden from the response.
    val displayNameJson = attr.displayInfos.foldLeft(Json.obj()) {
      case (acc, displayInfo) =>
        displayInfo.optionalName
          .map(name => acc.mergeIn(Json.obj(displayInfo.langtag -> name)))
          .getOrElse(acc)
    }

    val descriptionJson = attr.displayInfos.foldLeft(Json.obj()) {
      case (acc, displayInfo) =>
        displayInfo.optionalDescription
          .map(description => acc.mergeIn(Json.obj(displayInfo.langtag -> description)))
          .getOrElse(acc)
    }

    Json.obj(
      "name" -> attr.name,
      "displayName" -> displayNameJson,
      "description" -> descriptionJson,
      "kind" -> attr.kind.toString,
      "multilanguage" -> attr.multilanguage
    )
  }

  /**
    * A multilanguage attribute value is an object keyed by langtag, so without langtags there is no way to address one -
    * and the value migrations in ColumnModel would have nothing to reshape into or collapse from, which used to
    * silently destroy stored values. Rejecting the definition up front keeps that state unreachable.
    */
  def checkMultilanguageAllowed(langtags: Seq[String], definitions: Seq[LinkAttributeDefinition]): Unit = {
    if (langtags.isEmpty) {
      definitions.filter(_.multilanguage).foreach(definition =>
        throw UnprocessableEntityException(
          s"Link attribute '${definition.name}' can't be multilanguage because its table has no langtags."
        )
      )
    }
  }

  // Takes a count rather than the definitions so JsonUtils can reject an oversized array before it starts
  // validating individual entries, and still produce the exact same error as the model-level assertion.
  def checkMaxCount(count: Int): Unit = {
    if (count > maxCount) {
      throw InvalidJsonException(
        s"Only $maxCount linkAttributes entry is currently supported, but got $count.",
        "linkAttributes"
      )
    }
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

  /**
    * Canonical wire format for a `datetime` attribute value. Deliberately the Joda equivalent of ModelHelper's
    * `dateTimeFormat`, which is what a real datetime column is rendered with and what ColumnModel's kind migration
    * produces - a value has to look the same no matter whether it was written through the API or cast by a migration.
    * LinkAttributesTest.dateTimeValueIsNormalizedIdenticallyByWriteAndMigration pins the two together.
    */
  private val dateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

  def checkValidValue(
      definitions: Seq[LinkAttributeDefinition],
      attributes: JsonArray,
      allowedLangtags: Seq[String] = Seq.empty
  ): Try[Unit] = normalize(definitions, attributes, allowedLangtags).map(_ => ())

  /**
    * Validates a value array against its definitions and returns it in canonical form. Validating and normalizing are
    * the same pass on purpose: every kind that has more than one spelling for the same value (date, datetime) has to be
    * parsed to be checked anyway, and letting the parsed result fall on the floor is what allowed two spellings of one
    * instant to be stored side by side.
    *
    * `allowedLangtags` empty means "don't check langtag keys" - the caller either has no table langtags to check
    * against, or is a path where they aren't resolvable synchronously.
    */
  def normalize(
      definitions: Seq[LinkAttributeDefinition],
      attributes: JsonArray,
      allowedLangtags: Seq[String] = Seq.empty
  ): Try[JsonArray] = Try {
    val values = Option(attributes).map(_.asScala.toSeq).getOrElse(Seq.empty)

    if (values.size != definitions.size) {
      throw InvalidJsonException(
        s"Expected ${definitions.size} link attribute value(s) but got ${values.size}.",
        "link-attributes"
      )
    }

    val normalized = new JsonArray()

    definitions.zip(values).foreach {
      case (definition, rawValue) =>
        if (definition.multilanguage) {
          rawValue match {
            case null => normalized.addNull()
            case obj: JsonObject =>
              val normalizedObj = new JsonObject()

              obj.getMap.asScala.foreach({
                case (langtag, langValue) =>
                  checkLangtag(definition, langtag, allowedLangtags)

                  normalizeKindValue(definition, langValue) match {
                    case null => normalizedObj.putNull(langtag)
                    case value => normalizedObj.put(langtag, value)
                  }
              })

              normalized.add(normalizedObj)
            case other =>
              throw InvalidJsonException(
                s"Attribute '${definition.name}' is multilanguage and expects an object of langtag to value, but got $other.",
                "link-attributes"
              )
          }
        } else {
          rawValue match {
            case null => normalized.addNull()
            case _: JsonObject =>
              throw InvalidJsonException(
                s"Attribute '${definition.name}' is not multilanguage and expects a single value, but got an object.",
                "link-attributes"
              )
            case value =>
              normalizeKindValue(definition, value) match {
                case null => normalized.addNull()
                case normalizedValue => normalized.add(normalizedValue)
              }
          }
        }
    }

    normalized
  }

  private def checkLangtag(
      definition: LinkAttributeDefinition,
      langtag: String,
      allowedLangtags: Seq[String]
  ): Unit = {
    if (allowedLangtags.nonEmpty && !allowedLangtags.contains(langtag)) {
      throw InvalidJsonException(
        s"Langtag '$langtag' of attribute '${definition.name}' is not one of its table's langtags " +
          s"(${allowedLangtags.mkString(", ")}).",
        "link-attributes"
      )
    }
  }

  /**
    * Returns the value in canonical form for its kind, or null if it is cleared. Clearing is legal for every kind and
    * not a type violation: null means "no value (in this language)", which is what a multilanguage attribute with only
    * some langtags filled in looks like - and what a multilanguage flip leaves behind - so a value read back from the
    * API has to be acceptable as a write again.
    */
  private def normalizeKindValue(definition: LinkAttributeDefinition, value: Any): AnyRef = {
    // Every branch reports what it expected rather than letting a ClassCastException's message through - "class
    // java.lang.Integer cannot be cast to class java.lang.String" is not something to hand an API client.
    def expected(what: String): Nothing = throw new IllegalArgumentException(s"expected $what")

    def asString(what: String): String = value match {
      case s: String => s
      case _ => expected(what)
    }

    val result: Try[AnyRef] = if (value == null) {
      Success(null)
    } else {
      Try {
        definition.kind match {
          case TextType =>
            asString("a string")
          case NumericType =>
            value match {
              case n: Number => n
              case _ => expected("a number")
            }
          case IntegerType =>
            value match {
              case i: Integer => i
              case _ => expected("an integer")
            }
          case BooleanType =>
            value match {
              case b: java.lang.Boolean => b
              case _ => expected("a boolean")
            }
          case DateType =>
            LocalDate.parse(asString("a date string")).toString
          case DateTimeType =>
            DateTime.parse(asString("a datetime string")).withZone(DateTimeZone.UTC).toString(dateTimeFormat)
          case other =>
            throw new IllegalArgumentException(s"unsupported link attribute kind: $other")
        }
      }
    }

    result match {
      case Success(normalizedValue) => normalizedValue
      case Failure(ex) =>
        throw InvalidJsonException(
          s"Invalid value for attribute '${definition.name}' (${definition.kind}): ${ex.getMessage}",
          "link-attributes"
        )
    }
  }
}
