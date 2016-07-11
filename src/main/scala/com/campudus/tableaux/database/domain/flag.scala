package com.campudus.tableaux.database.domain

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
  final val TRANSLATED = "translated"
  final val FROZEN = "frozen"

  def apply(flagStr: String): CellFlagType = flagStr match {
    case CellFlagType.TRANSLATED => TranslatedFlagType
    case CellFlagType.FROZEN => FrozenFlagType
    case _ => throw new IllegalArgumentException(s"Invalid cell flag type $flagStr")
  }
}

sealed trait CellFlagType {
  def toString: String
}

case object TranslatedFlagType extends CellFlagType {
  override def toString: String = CellFlagType.TRANSLATED
}

case object FrozenFlagType extends CellFlagType {
  override def toString: String = CellFlagType.FROZEN
}

object CellLevelFlag {
  def apply(flags: JsonArray): Map[ColumnId, Seq[CellLevelFlag]] = {
    flags.asScala.toSeq.map({
      case obj: JsonObject =>
        val columnId = obj.getLong("column_id")
        obj.remove("column_id")

        val langtag: Option[String] = if ("neutral".equals(obj.getString("langtag", "neutral"))) {
          obj.remove("langtag")
          None
        } else {
          Some(obj.getString("langtag"))
        }

        val flagType = CellFlagType(obj.getString("type"))

        val value = obj.getString("value")

        val createdAt = DateTime.parse(obj.getString("created_at"))

        (columnId, CellLevelFlag(langtag, flagType, value, createdAt))
    }).groupBy(_._1).map({
      case (columnId, flagsAsTupleSeq) =>
        (columnId.toLong, flagsAsTupleSeq.map(_._2))
    })
  }
}

case class CellLevelFlag(langtag: Option[String], flagType: CellFlagType, value: String, createdAt: DateTime) extends DomainObject {
  override def getJson: JsonObject = {
    val json = Json.obj(
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