package com.campudus.tableaux.database.domain

import java.util.UUID

import com.campudus.tableaux.database.model.TableauxModel.ColumnId
import org.joda.time.DateTime
import org.vertx.scala.core.json._

import scala.collection.JavaConverters._

object RowLevelFlags {
  def apply(finalFlag: Boolean, needsTranslationFlags: JsonArray): RowLevelFlags = {
    val seq = needsTranslationFlags.asScala.toSeq.map({ case langtag: String => langtag })
    RowLevelFlags(finalFlag, seq)
  }
}

case class RowLevelFlags(finalFlag: Boolean, needsTranslationFlags: Seq[String]) extends DomainObject {
  override def getJson: JsonObject = Json.obj(
    "final" -> finalFlag,
    "needsTranslation" -> compatibilityGet(needsTranslationFlags)
  )
}

object CellFlagType {
  final val ERROR = "error"
  final val WARNING = "warning"
  final val INFO = "info"

  def apply(flagStr: String): CellFlagType = flagStr match {
    case CellFlagType.ERROR => ErrorFlagType
    case CellFlagType.WARNING => WarningFlagType
    case CellFlagType.INFO => InfoFlagType
    case _ => throw new IllegalArgumentException(s"Invalid cell flag type $flagStr")
  }
}

sealed trait CellFlagType {
  def toString: String
}

case object ErrorFlagType extends CellFlagType {
  override def toString: String = CellFlagType.ERROR
}

case object WarningFlagType extends CellFlagType {
  override def toString: String = CellFlagType.WARNING
}

case object InfoFlagType extends CellFlagType {
  override def toString: String = CellFlagType.INFO
}

object CellLevelFlag {
  def apply(flags: JsonArray): Map[ColumnId, Seq[CellLevelFlag]] = {
    flags
      .asScala
      .toSeq
      .map({
      case obj: JsonObject =>
        val columnId = obj.getLong("column_id")
        obj.remove("column_id")

        val langtag: Option[String] = if ("neutral".equals(obj.getString("langtag", "neutral"))) {
          obj.remove("langtag")
          None
        } else {
          Some(obj.getString("langtag"))
        }

        val uuid = obj.getString("uuid")
        val flagType = CellFlagType(obj.getString("type"))
        val value = obj.getString("value")
        val createdAt = DateTime.parse(obj.getString("created_at"))

        (columnId, CellLevelFlag(UUID.fromString(uuid), langtag, flagType, value, createdAt))
      })
      .groupBy({
        case (columnId, _) => columnId
      })
      .map({
        case (columnId, flagsAsTupleSeq) => (columnId.toLong, flagsAsTupleSeq.map({ case (_, flagSeq) => flagSeq }))
      })
  }
}

case class CellLevelFlag(uuid: UUID, langtag: Option[String], flagType: CellFlagType, value: String, createdAt: DateTime) extends DomainObject {
  override def getJson: JsonObject = {
    val json = Json.obj(
      "uuid" -> uuid.toString,
      "type" -> flagType.toString,
      "value" -> value,
      "created_at" -> createdAt.toString()
    )

    if (langtag.isDefined) {
      json.put("langtag", langtag.orNull)
    }

    json
  }
}

case class CellLevelFlags(columns: Seq[ColumnType[_]], flags: Map[ColumnId, Seq[CellLevelFlag]]) extends DomainObject {
  override def getJson: JsonObject = {
    val seqOpt = columns.map({
      column =>
        flags.get(column.id)
    })

    Json.obj(
      "cellFlags" -> compatibilityGet(seqOpt)
    )
  }
}