package com.campudus.tableaux.database.domain

import com.campudus.tableaux.{ArgumentChecker, InvalidJsonException, OkArg}
import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.database.{LanguageNeutral, _}
import com.campudus.tableaux.database.model.AttachmentFile
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}

import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.core.json._

import scala.util.{Failure, Success, Try}

import com.typesafe.scalalogging.LazyLogging
import java.util.UUID
import org.joda.time.{DateTime, LocalDate}

case class DependentColumnInformation(
    tableId: TableId,
    id: ColumnId,
    kind: TableauxDbType,
    identifier: Boolean,
    groupColumnIds: Seq[ColumnId]
)

sealed trait ColumnInformation {
  val table: Table
  val id: ColumnId
  val name: String
  val ordering: Ordering
  val identifier: Boolean
  val displayInfos: Seq[DisplayInfo]
  val groupColumnIds: Seq[ColumnId]
  val separator: Boolean
  val attributes: JsonObject
  val hidden: Boolean
  val maxLength: Option[Int]
  val minLength: Option[Int]
  val decimalDigits: Option[Int]
}

object BasicColumnInformation {

  def apply(
      table: Table,
      columnId: ColumnId,
      ordering: Ordering,
      displayInfos: Seq[DisplayInfo],
      createColumn: CreateColumn
  ): BasicColumnInformation = {
    val attributes = createColumn.attributes match {
      case Some(obj) => obj
      case None => new JsonObject("{}")
    }
    BasicColumnInformation(
      table,
      columnId,
      createColumn.name,
      ordering,
      createColumn.identifier,
      displayInfos,
      Seq.empty,
      createColumn.separator,
      attributes,
      createColumn.hidden,
      createColumn.maxLength,
      createColumn.minLength,
      createColumn.decimalDigits
    )
  }
}

object StatusColumnInformation {

  def apply(
      table: Table,
      columnId: ColumnId,
      ordering: Ordering,
      displayInfos: Seq[DisplayInfo],
      createColumn: CreateStatusColumn
  ): StatusColumnInformation = {
    val attributes = createColumn.attributes match {
      case Some(obj) => obj
      case None => new JsonObject("{}")
    }
    StatusColumnInformation(
      table,
      columnId,
      createColumn.name,
      ordering,
      createColumn.identifier,
      displayInfos,
      Seq.empty,
      createColumn.separator,
      attributes,
      createColumn.rules,
      createColumn.hidden
    )
  }
}

case class BasicColumnInformation(
    override val table: Table,
    override val id: ColumnId,
    override val name: String,
    override val ordering: Ordering,
    override val identifier: Boolean,
    override val displayInfos: Seq[DisplayInfo],
    override val groupColumnIds: Seq[ColumnId],
    override val separator: Boolean,
    override val attributes: JsonObject,
    override val hidden: Boolean,
    override val maxLength: Option[Int],
    override val minLength: Option[Int],
    override val decimalDigits: Option[Int]
) extends ColumnInformation

case class StatusColumnInformation(
    override val table: Table,
    override val id: ColumnId,
    override val name: String,
    override val ordering: Ordering,
    override val identifier: Boolean,
    override val displayInfos: Seq[DisplayInfo],
    override val groupColumnIds: Seq[ColumnId],
    override val separator: Boolean,
    override val attributes: JsonObject,
    val rules: JsonArray,
    override val hidden: Boolean,
    override val maxLength: Option[Int] = None,
    override val minLength: Option[Int] = None,
    override val decimalDigits: Option[Int] = None
) extends ColumnInformation

case class ConcatColumnInformation(override val table: Table) extends ColumnInformation {
  override val name = "ID"

  // Right now, every concat column is
  // an identifier
  override val identifier = true

  override val id: ColumnId = 0
  override val ordering: Ordering = 0
  override val displayInfos: Seq[DisplayInfo] = List()

  // ConcatColumn can't be grouped
  override val groupColumnIds: Seq[ColumnId] = Seq.empty
  override val separator: Boolean = false
  override val hidden: Boolean = false
  override val attributes: JsonObject = Json.obj()
  override val maxLength: Option[Int] = None
  override val minLength: Option[Int] = None
  override val decimalDigits: Option[Int] = None
}

object ColumnType {

  private type MultiLanguageAndValue = (SimpleValueColumn[_], Map[String, Option[_]])
  private type LanguageNeutralAndValue = (SimpleValueColumn[_], Option[_])
  private type LinkAndRowIds = (LinkColumn, Seq[RowId])
  private type AttachmentAndUUIDs = (AttachmentColumn, Seq[(UUID, Option[Ordering])])

  /**
    * Splits Seq of columns into column types and parses for correct value depending on column type.
    *
    *   - language-neutral
    *   - multi-language and/or multi-country
    *   - link
    *   - attachment
    */
  def splitIntoTypesWithValues(columnsWithValue: Seq[(ColumnType[_], _)]): Try[
    (List[LanguageNeutralAndValue], List[MultiLanguageAndValue], List[LinkAndRowIds], List[AttachmentAndUUIDs])
  ] = {
    Try {
      columnsWithValue.foldLeft(
        (
          List.empty[LanguageNeutralAndValue],
          List.empty[MultiLanguageAndValue],
          List.empty[LinkAndRowIds],
          List.empty[AttachmentAndUUIDs]
        )
      ) {

        case ((s, m, l, a), (MultiLanguageColumn(c), v)) =>
          (s, (c, MultiLanguageColumn.checkValidValue(c, v).get) :: m, l, a)

        case ((s, m, l, a), (c: SimpleValueColumn[_], v)) =>
          ((c, c.checkValidValue(v).get) :: s, m, l, a)

        case ((s, m, l, a), (c: LinkColumn, v)) =>
          (s, m, (c, c.checkValidValue(v).get.orNull) :: l, a)

        case ((s, m, l, a), (c: AttachmentColumn, v)) =>
          (s, m, l, (c, c.checkValidValue(v).get.orNull) :: a)

        case (_, (c, v)) =>
          throw new ClassCastException(s"unknown column or value: $c -> $v")
      }
    }
  }

  /**
    * Splits Seq of columns into column types.
    *
    *   - language-neutral
    *   - multi-language and/or multi-country
    *   - link
    *   - attachment
    */
  def splitIntoTypes(
      columns: Seq[ColumnType[_]]
  ): (List[SimpleValueColumn[_]], List[SimpleValueColumn[_]], List[LinkColumn], List[AttachmentColumn]) = {
    columns.foldLeft(
      (
        List[SimpleValueColumn[_]](),
        List[SimpleValueColumn[_]](),
        List[LinkColumn](),
        List[AttachmentColumn]()
      )
    ) {

      case ((s, m, l, a), MultiLanguageColumn(c)) =>
        (s, c :: m, l, a)

      case ((s, m, l, a), c: SimpleValueColumn[_]) =>
        (c :: s, m, l, a)

      case ((s, m, l, a), c: LinkColumn) =>
        (s, m, c :: l, a)

      case ((s, m, l, a), c: AttachmentColumn) =>
        (s, m, l, c :: a)
    }
  }
}

sealed trait ColumnType[+A] extends DomainObject {

  val kind: TableauxDbType

  val languageType: LanguageType

  val columnInformation: ColumnInformation

  final val table: Table = columnInformation.table

  final val id: ColumnId = columnInformation.id

  final val name: String = columnInformation.name

  final val ordering: Ordering = columnInformation.ordering

  final val identifier: Boolean = columnInformation.identifier

  val separator: Boolean = columnInformation.separator
  val attributes: JsonObject = columnInformation.attributes
  val hidden: Boolean = columnInformation.hidden

  protected[this] implicit def roleModel: RoleModel

  override def getJson: JsonObject = {

    // backward compatibility
    val multilanguage = languageType != LanguageNeutral

    val json = Json.obj(
      "id" -> id,
      "ordering" -> ordering,
      "name" -> name,
      "kind" -> kind.toString,
      "multilanguage" -> multilanguage,
      "identifier" -> identifier,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "separator" -> separator,
      "attributes" -> attributes,
      "hidden" -> hidden
    )

    languageType match {
      case MultiLanguage =>
        json.mergeIn(Json.obj("languageType" -> LanguageType.LANGUAGE))

      case MultiCountry(countryCodes) =>
        json.mergeIn(
          Json.obj(
            "languageType" -> LanguageType.COUNTRY,
            "countryCodes" -> Json.arr(countryCodes.codes: _*)
          )
        )

      case _ =>
      // do nothing
    }

    columnInformation.displayInfos.foreach(displayInfo => {
      displayInfo.optionalName.map(name => {
        json
          .mergeIn(Json.obj("displayName" -> json.getJsonObject("displayName")
            .mergeIn(Json.obj(displayInfo.langtag -> name))))
      })

      displayInfo.optionalDescription.map(desc => {
        json
          .mergeIn(Json.obj("description" -> json.getJsonObject("description")
            .mergeIn(Json.obj(displayInfo.langtag -> desc))))
      })
    })
    json
  }

  def checkValidValue[B](value: B): Try[Option[A]]

  protected def getJsonPart(columnInformationFiled: Option[Any], name: String): JsonObject = {
    columnInformationFiled match {
      case Some(v) => Json.obj(name -> v)
      case None => Json.emptyObj()
    }
  }
}

/**
  * Helper for pattern matching
  */
object MultiLanguageColumn {

  def checkValidValue[A, B](columnType: ColumnType[A], value: B): Try[Map[String, Option[A]]] = {
    Option(value) match {
      case None =>
        Success(Map.empty)
      case Some(json: JsonObject) =>
        Try[Map[String, Option[A]]] {
          json.asMap
            .map({
              case (key: String, v) =>
                (key, columnType.checkValidValue(v))
            })
            .collect({
              case (key: String, Success(castedValue)) => (key, castedValue)
              case (key: String, Failure(ex)) =>
                columnType.languageType match {
                  case MultiLanguage =>
                    throw new IllegalArgumentException(
                      s"Invalid value at key $key for MultiLanguage column ${columnType.name}",
                      ex
                    )
                  case MultiCountry(_) | LanguageNeutral =>
                    throw new IllegalArgumentException(
                      s"Invalid value at key $key for MultiCountry column ${columnType.name}",
                      ex
                    )
                }
            })
            .toMap
        }
      case _ =>
        throw new IllegalArgumentException(s"Invalid value (JSON required) for MultiLanguage column ${columnType.name}")
    }
  }

  def unapply(columnType: ColumnType[_]): Option[SimpleValueColumn[_]] = {
    (columnType, columnType.languageType) match {
      case (simpleValueColumn: SimpleValueColumn[_], MultiLanguage | MultiCountry(_)) => Some(simpleValueColumn)
      case _ => None
    }
  }
}

object SimpleValueColumn {

  def apply(
      kind: TableauxDbType,
      languageType: LanguageType,
      columnInformation: ColumnInformation
  )(implicit roleModel: RoleModel = RoleModel(), user: TableauxUser): SimpleValueColumn[_] = {
    val applyFn: LanguageType => ColumnInformation => SimpleValueColumn[_] = kind match {
      case TextType => TextColumn.apply
      case RichTextType => RichTextColumn.apply
      case ShortTextType => ShortTextColumn.apply
      case NumericType => NumberColumn.apply
      case CurrencyType => CurrencyColumn.apply
      case BooleanType => BooleanColumn.apply
      case DateType => DateColumn.apply
      case DateTimeType => DateTimeColumn.apply
      case IntegerType => IntegerColumn.apply

      case _ => throw new IllegalArgumentException("Can only map type to SimpleValueColumn")
    }

    applyFn(languageType)(columnInformation)
  }
}

/**
  * Base class for all primitive column types
  */
sealed abstract class SimpleValueColumn[+A](override val kind: TableauxDbType)(override val languageType: LanguageType)(
    implicit val roleModel: RoleModel,
    user: TableauxUser
) extends ColumnType[A] {

  override def checkValidValue[B](value: B): Try[Option[A]] = {
    Option(value) match {
      case None => Success(None)
      case _ => checkValidSingleValue(value).map(Some(_))
    }
  }

  protected[this] def checkValidSingleValue[B](value: B): Try[A]
}

case class TextColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation)(
    implicit override val roleModel: RoleModel,
    val user: TableauxUser
) extends SimpleValueColumn[String](TextType)(languageType) {

  override def checkValidSingleValue[B](value: B): Try[String] = Try(value.asInstanceOf[String])

  override def getJson: JsonObject = {
    val maxLengthJson = getJsonPart(columnInformation.maxLength, "maxLength")
    val minLengthJson = getJsonPart(columnInformation.minLength, "minLength")

    super.getJson
      .mergeIn(maxLengthJson)
      .mergeIn(minLengthJson)
  }
}

case class ShortTextColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation)(
    implicit override val roleModel: RoleModel,
    val user: TableauxUser
) extends SimpleValueColumn[String](ShortTextType)(languageType) {

  override def checkValidSingleValue[B](value: B): Try[String] = Try(value.asInstanceOf[String])

  override def getJson: JsonObject = {
    val maxLengthJson = getJsonPart(columnInformation.maxLength, "maxLength")
    val minLengthJson = getJsonPart(columnInformation.minLength, "minLength")

    super.getJson
      .mergeIn(maxLengthJson)
      .mergeIn(minLengthJson)
  }
}

case class RichTextColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation)(
    implicit override val roleModel: RoleModel,
    val user: TableauxUser
) extends SimpleValueColumn[String](RichTextType)(languageType) {

  override def checkValidSingleValue[B](value: B): Try[String] = Try(value.asInstanceOf[String])

  override def getJson: JsonObject = {
    val maxLengthJson = getJsonPart(columnInformation.maxLength, "maxLength")
    val minLengthJson = getJsonPart(columnInformation.minLength, "minLength")

    super.getJson
      .mergeIn(maxLengthJson)
      .mergeIn(minLengthJson)
  }
}

case class NumberColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation)(
    implicit override val roleModel: RoleModel,
    val user: TableauxUser
) extends SimpleValueColumn[Number](NumericType)(languageType) {

  override def getJson: JsonObject = {
    val decimalDigitsJson = getJsonPart(columnInformation.decimalDigits, "decimalDigits")

    super.getJson
      .mergeIn(decimalDigitsJson)
  }
  override def checkValidSingleValue[B](value: B): Try[Number] = Try(value.asInstanceOf[Number])
}

case class IntegerColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation)(
    implicit override val roleModel: RoleModel,
    val user: TableauxUser
) extends SimpleValueColumn[Number](IntegerType)(languageType) {

  override def checkValidSingleValue[B](value: B): Try[Number] = Try(value.asInstanceOf[Integer])
}

case class CurrencyColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation)(
    implicit override val roleModel: RoleModel,
    val user: TableauxUser
) extends SimpleValueColumn[Number](CurrencyType)(languageType) {

  override def checkValidSingleValue[B](value: B): Try[Number] = Try(value.asInstanceOf[Number])
}

case class BooleanColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation)(
    implicit override val roleModel: RoleModel,
    val user: TableauxUser
) extends SimpleValueColumn[Boolean](BooleanType)(languageType) {

  override def checkValidSingleValue[B](value: B): Try[Boolean] = Try(value.asInstanceOf[Boolean])
}

case class DateColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation)(
    implicit override val roleModel: RoleModel,
    val user: TableauxUser
) extends SimpleValueColumn[String](DateType)(languageType) {

  override def checkValidSingleValue[B](value: B): Try[String] = {
    Try(LocalDate.parse(value.asInstanceOf[String])).flatMap(_ => Try(value.asInstanceOf[String]))
  }
}

case class DateTimeColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation)(
    implicit override val roleModel: RoleModel,
    val user: TableauxUser
) extends SimpleValueColumn[String](DateTimeType)(languageType) {

  override def checkValidSingleValue[B](value: B): Try[String] = {
    Try(DateTime.parse(value.asInstanceOf[String])).flatMap(_ => Try(value.asInstanceOf[String]))
  }
}

/*
 * Special column types
 */
case class LinkColumn(
    override val columnInformation: ColumnInformation,
    to: ColumnType[_],
    linkId: LinkId,
    linkDirection: LinkDirection
)(implicit override val roleModel: RoleModel, val user: TableauxUser) extends ColumnType[Seq[RowId]]
    with LazyLogging {
  override val kind: LinkType.type = LinkType
  override val languageType: LanguageType = to.languageType

  override def getJson: JsonObject = {
    val baseJson = Json.obj(
      "toTable" -> to.table.id,
      "toColumn" -> to.getJson
    )

    val constraintJson = linkDirection.constraint.getJson match {
      case json if json.isEmpty => Json.emptyObj()
      case json => Json.obj("constraint" -> json)
    }

    super.getJson
      .mergeIn(baseJson)
      .mergeIn(constraintJson)
  }

  override def checkValidValue[B](value: B): Try[Option[Seq[RowId]]] = {
    Try {
      val castedValue = value match {
        case x if Option(x).isEmpty =>
          Seq.empty[Long]

        case x: Int =>
          Seq(x.toLong)

        case x: Seq[_] =>
          x.map {
            case id: RowId => id
            case obj: JsonObject => obj.getLong("id").longValue()
          }

        case x: JsonObject if x.containsKey("to") =>
          import ArgumentChecker._
          hasLong("to", x) match {
            case arg: OkArg[Long] =>
              Seq(arg.get)
            case _ =>
              throw InvalidJsonException(
                s"A link column expects a JSON object with to values, but got $x",
                "link-value"
              )
          }

        case x: JsonObject if x.containsKey("values") =>
          import scala.collection.JavaConverters._
          Try(
            checked(hasArray("values", x)).asScala
              .map(_.asInstanceOf[java.lang.Integer].longValue())
              .toSeq
          ) match {
            case Success(ids) =>
              ids
            case Failure(_) =>
              throw InvalidJsonException(
                s"A link column expects a JSON object with to values, but got $x",
                "link-value"
              )
          }

        case x: JsonObject =>
          throw InvalidJsonException(s"A link column expects a JSON object with to values, but got $x", "link-value")

        case x: JsonArray =>
          import scala.collection.JavaConverters._
          x.asScala
            .map({
              // need to check for java.lang.Integer because we are mapping over AnyRefs
              case id: Integer => id.toLong
              case obj: JsonObject => obj.getLong("id").toLong
            })
            .toSeq

        case x =>
          throw InvalidJsonException(s"A link column expects a JSON object with values, but got $x", "link-value")
      }

      Some(castedValue)
    }
  }
}

case class StatusColumn(
    override val columnInformation: ColumnInformation,
    rules: JsonArray,
    override val columns: Seq[ColumnType[_]]
)(implicit override val roleModel: RoleModel, val user: TableauxUser) extends ConcatenateColumn
    with LazyLogging {

  override val languageType: LanguageNeutral.type = LanguageNeutral
  override val kind = StatusType

  override def getJson: JsonObject = {
    super.getJson
      .mergeIn(
        Json.obj(
          "rules" -> rules
        )
      )
  }
}

object StatusColumn {
  val validColumnTypes: Seq[TableauxDbType] = Seq(BooleanType, RichTextType, ShortTextType, TextType, NumericType)
}

case class AttachmentColumn(override val columnInformation: ColumnInformation)(
    implicit override val roleModel: RoleModel,
    val user: TableauxUser
) extends ColumnType[Seq[(UUID, Option[Ordering])]] {
  override val kind: AttachmentType.type = AttachmentType
  override val languageType: LanguageNeutral.type = LanguageNeutral

  override def checkValidValue[B](value: B): Try[Option[Seq[(UUID, Option[Ordering])]]] = {
    Try {
      val castedValue = value match {
        case uuid: String =>
          notNull(uuid, "uuid")
          Seq((UUID.fromString(uuid), None))

        case attachment: JsonObject =>
          notNull(attachment.getString("uuid"), "uuid")
          Seq((UUID.fromString(attachment.getString("uuid")), Option(attachment.getLong("ordering")).map(_.toLong)))

        case attachments: JsonArray =>
          import scala.collection.JavaConverters._
          attachments.asScala
            .map({
              case attachment: JsonObject =>
                notNull(attachment.getString("uuid"), "uuid")
                (UUID.fromString(attachment.getString("uuid")), Option(attachment.getLong("ordering")).map(_.toLong))

              case uuid: String =>
                (UUID.fromString(uuid), None)
            })
            .toSeq

        case attachments: Stream[_] =>
          attachments.map({
            case file: AttachmentFile =>
              (file.file.file.uuid, Some(file.ordering))
          })
      }

      Some(castedValue)
    }
  }
}

sealed trait ConcatenateColumn extends ColumnType[JsonArray] {
  val columns: Seq[ColumnType[_]]

  // If any of the columns is MultiLanguage or MultiCountry
  // the ConcatColumn will be MultiLanguage
  override val languageType: LanguageType = {
    val isMultiLanguageOrMultiCountry = columns.exists(_.languageType match {
      case MultiLanguage | MultiCountry(_) => true
      case _ => false
    })

    if (isMultiLanguageOrMultiCountry) {
      MultiLanguage
    } else {
      LanguageNeutral
    }
  }

  override def checkValidValue[B](value: B): Try[Option[JsonArray]] = {
    Failure(new IllegalArgumentException(s"Cannot set a value for ${className()}. Value will be generated."))
  }

  private def className() = {
    this.getClass.getCanonicalName.split("\\.").toList.lastOption.getOrElse("~Unknown~")
  }
}

case class ConcatColumn(
    override val columnInformation: ConcatColumnInformation,
    override val columns: Seq[ColumnType[_]],
    formatPattern: Option[String]
)(implicit override val roleModel: RoleModel, val user: TableauxUser) extends ConcatenateColumn {
  override val kind: ConcatType.type = ConcatType

  override def getJson: JsonObject = {
    val concatsJson = Json.obj("concats" -> columns.map(_.getJson))

    val formatPatternJson = formatPattern match {
      case Some(pattern) => Json.obj("formatPattern" -> pattern)
      case None => Json.emptyObj()
    }

    super.getJson
      .mergeIn(concatsJson)
      .mergeIn(formatPatternJson)
  }
}

case class GroupColumn(
    override val columnInformation: ColumnInformation,
    override val columns: Seq[ColumnType[_]],
    formatPattern: Option[String],
    showMemberColumns: Boolean
)(implicit override val roleModel: RoleModel, val user: TableauxUser) extends ConcatenateColumn {
  override val kind: GroupType.type = GroupType

  override def getJson: JsonObject = {
    val groupsJson = Json.obj("groups" -> columns.map(_.getJson))

    val formatPatternJson = formatPattern match {
      case Some(pattern) => Json.obj("formatPattern" -> pattern)
      case None => Json.emptyObj()
    }

    val showMemberColumnsJson = showMemberColumns match {
      case true => Json.obj("showMemberColumns" -> showMemberColumns)
      case false => Json.emptyObj()
    }

    super.getJson
      .mergeIn(groupsJson)
      .mergeIn(formatPatternJson)
      .mergeIn(showMemberColumnsJson)
  }
}

/**
  * Column seq is just a sequence of columns.
  *
  * @param columns
  *   The sequence of columns.
  */
case class ColumnSeq(columns: Seq[ColumnType[_]])(implicit roleModel: RoleModel, val user: TableauxUser)
    extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj("columns" -> columns.map(_.getJson))
  }
}
