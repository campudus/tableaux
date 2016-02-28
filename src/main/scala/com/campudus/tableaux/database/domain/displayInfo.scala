package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

sealed trait DisplayInfo {
  val langtag: String
  val optionalName: Option[String]
  val optionalDescription: Option[String]
}

case class NameOnly(langtag: String, name: String) extends DisplayInfo {
  val optionalName = Some(name)
  val optionalDescription = None
}

case class NameAndDescription(langtag: String, name: String, description: String) extends DisplayInfo {
  val optionalName = Some(name)
  val optionalDescription = Some(description)
}

case class DescriptionOnly(langtag: String, description: String) extends DisplayInfo {
  val optionalName = None
  val optionalDescription = Some(description)
}

class DisplayInfos(tableId: TableId, columnId: ColumnId, val entries: Seq[DisplayInfo]) {

  def nonEmpty: Boolean = entries.nonEmpty

  def statement: String = {
    s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
        |VALUES ${entries.map(_ => s"(?, ?, ?, ?, ?)").mkString(", ")}""".stripMargin
  }

  def binds: Seq[Any] = entries.flatMap {
    di => List(tableId, columnId) ::: List(di.langtag, di.optionalName.orNull, di.optionalDescription.orNull)
  }

}

object DisplayInfos {
  def allInfos(json: JsonObject): Seq[DisplayInfo] = {
    import scala.collection.JavaConverters._
    val names = json.getJsonObject("displayName", Json.obj()).fieldNames().asScala
    val descriptions = json.getJsonObject("description", Json.obj()).fieldNames().asScala
    val both = names.intersect(descriptions) map { lang =>
      NameAndDescription(lang,
        json.getJsonObject("displayName").getString(lang),
        json.getJsonObject("description").getString(lang))
    }
    val nameOnly = names.diff(descriptions) map { lang =>
      NameOnly(lang, json.getJsonObject("displayName").getString(lang))
    }
    val descOnly = descriptions.diff(names) map { lang =>
      DescriptionOnly(lang, json.getJsonObject("description").getString(lang))
    }

    both.toList ::: nameOnly.toList ::: descOnly.toList
  }

  def fromString(langtag: String, name: String, description: String): DisplayInfo = (langtag, name, description) match {
    case (_, name, null) => NameOnly(langtag, name)
    case (_, null, desc) => DescriptionOnly(langtag, description)
    case (_, name, desc) => NameAndDescription(langtag, name, description)
  }

  def apply(tableId: TableId, columnId: ColumnId, entries: Seq[DisplayInfo]): DisplayInfos = {
    new DisplayInfos(tableId, columnId, entries)
  }

  def apply(tableId: TableId, columnId: ColumnId, json: JsonObject): DisplayInfos =
    apply(tableId, columnId, allInfos(json))
}
