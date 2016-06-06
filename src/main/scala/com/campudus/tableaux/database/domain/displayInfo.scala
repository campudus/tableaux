package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.domain.DisplayInfos._
import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

sealed trait DisplayInfo {
  val langtag: Langtag
  val optionalName: Option[String]
  val optionalDescription: Option[String]
}

case class NameOnly(langtag: Langtag, name: String) extends DisplayInfo{
  val optionalName = Some(name)
  val optionalDescription = None
}

case class DescriptionOnly(langtag: Langtag, description: String) extends DisplayInfo{
  val optionalName = None
  val optionalDescription = Some(description)
}

case class NameAndDescription(langtag: Langtag, name: String, description: String) extends DisplayInfo {
  val optionalName = Some(name)
  val optionalDescription = Some(description)
}

class ColumnDisplayInfos(tableId: TableId, columnId: ColumnId, val entries: Seq[DisplayInfo]) {

  def nonEmpty: Boolean = entries.nonEmpty

  def statement: String = {
    s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
        |VALUES ${entries.map(_ => s"(?, ?, ?, ?, ?)").mkString(", ")}""".stripMargin
  }

  def binds: Seq[Any] = entries.flatMap {
    di => List(tableId, columnId, di.langtag, di.optionalName.orNull, di.optionalDescription.orNull)
  }

}

class TableDisplayInfos(tableId: TableId, val entries: Seq[DisplayInfo]) {

  def nonEmpty: Boolean = entries.nonEmpty

  def createSql: (String, Seq[Any]) = (
    s"INSERT INTO system_table_lang (table_id, langtag, name, description) VALUES ${entries.map(_ => "(?, ?, ?, ?)").mkString(", ")}",
    entries.flatMap(di => List(tableId, di.langtag, di.optionalName.orNull, di.optionalDescription.orNull))
    )

  def insertSql: Map[Langtag, (String, Seq[Any])] = entries.foldLeft(Map[Langtag, (String, Seq[Any])]()) {
    case (m, NameOnly(langtag, name)) =>
      m + (langtag ->(
        s"INSERT INTO system_table_lang (table_id, langtag, name) VALUES (?, ?, ?)",
        Seq(tableId, langtag, name)
        ))
    case (m, DescriptionOnly(langtag, description)) =>
      m + (langtag ->(
        s"INSERT INTO system_table_lang (table_id, langtag, description) VALUES (?, ?, ?)",
        Seq(tableId, langtag, description)
        ))
    case (m, NameAndDescription(langtag, name, description)) =>
      m + (langtag ->(
        s"INSERT INTO system_table_lang (table_id, langtag, name, description) VALUES (?, ?, ?, ?)",
        Seq(tableId, langtag, name, description)
        ))
  }

  def updateSql: Map[Langtag, (String, Seq[Any])] = entries.foldLeft(Map[Langtag, (String, Seq[Any])]()) {
    case (m, NameOnly(langtag, name)) =>
      m + (langtag ->(
        s"UPDATE system_table_lang SET name = ? WHERE table_id = ? AND langtag = ?",
        Seq(name, tableId, langtag)
        ))
    case (m, DescriptionOnly(langtag, description)) =>
      m + (langtag ->(
        s"UPDATE system_table_lang SET description = ? WHERE table_id = ? AND langtag = ?",
        Seq(description, tableId, langtag)
        ))
    case (m, NameAndDescription(langtag, name, description)) =>
      m + (langtag ->(
        s"UPDATE system_table_lang SET name = ?, description = ? WHERE table_id = ? AND langtag = ?",
        Seq(name, description, tableId, langtag)
        ))
  }

}

object DisplayInfos {
  type Langtag = String

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

  def apply(tableId: TableId, entries: Seq[DisplayInfo]): TableDisplayInfos =
    new TableDisplayInfos(tableId, entries)

  def apply(tableId: TableId, json: JsonObject): TableDisplayInfos =
    apply(tableId, allInfos(json))

  def apply(tableId: TableId, columnId: ColumnId, entries: Seq[DisplayInfo]): ColumnDisplayInfos =
    new ColumnDisplayInfos(tableId, columnId, entries)

  def apply(tableId: TableId, columnId: ColumnId, json: JsonObject): ColumnDisplayInfos =
    apply(tableId, columnId, allInfos(json))
}
