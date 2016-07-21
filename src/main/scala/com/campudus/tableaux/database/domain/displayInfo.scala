package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.domain.DisplayInfos._
import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

sealed trait DisplayInfos {
  def nonEmpty: Boolean

  val entries: Seq[DisplayInfo]
}

sealed trait DisplayInfo {
  val langtag: Langtag
  val optionalName: Option[String]
  val optionalDescription: Option[String]
}

object DisplayInfo {
  def unapply(displayInfo: DisplayInfo): Option[(Langtag, Option[String], Option[String])] = {
    Some(displayInfo.langtag, displayInfo.optionalName, displayInfo.optionalDescription)
  }
}

case class NameOnly(langtag: Langtag, name: String) extends DisplayInfo {
  val optionalName = Some(name)
  val optionalDescription = None
}

case class DescriptionOnly(langtag: Langtag, description: String) extends DisplayInfo {
  val optionalName = None
  val optionalDescription = Some(description)
}

case class NameAndDescription(langtag: Langtag, name: String, description: String) extends DisplayInfo {
  val optionalName = Some(name)
  val optionalDescription = Some(description)
}

class TableGroupDisplayInfos(tableGroupId: TableId, override val entries: Seq[DisplayInfo]) extends DisplayInfos {

  override def nonEmpty: Boolean = entries.nonEmpty

  def createSql: (String, Seq[Any]) = (
    s"INSERT INTO system_tablegroup_lang (id, langtag, name, description) VALUES ${entries.map(_ => "(?, ?, ?, ?)").mkString(", ")}",
    entries.flatMap(di => List(tableGroupId, di.langtag, di.optionalName.orNull, di.optionalDescription.orNull))
    )

  def insertSql: Map[Langtag, (String, Seq[Any])] = entries.foldLeft(Map[Langtag, (String, Seq[Any])]()) {
    case (m, NameOnly(langtag, name)) =>
      m + (langtag -> (
        s"INSERT INTO system_tablegroup_lang (id, langtag, name) VALUES (?, ?, ?)",
        Seq(tableGroupId, langtag, name)
        ))
    case (m, DescriptionOnly(langtag, description)) =>
      m + (langtag -> (
        s"INSERT INTO system_tablegroup_lang (id, langtag, description) VALUES (?, ?, ?)",
        Seq(tableGroupId, langtag, description)
        ))
    case (m, NameAndDescription(langtag, name, description)) =>
      m + (langtag -> (
        s"INSERT INTO system_tablegroup_lang (id, langtag, name, description) VALUES (?, ?, ?, ?)",
        Seq(tableGroupId, langtag, name, description)
        ))
  }

  def updateSql: Map[Langtag, (String, Seq[Any])] = entries.foldLeft(Map[Langtag, (String, Seq[Any])]()) {
    case (m, NameOnly(langtag, name)) =>
      m + (langtag -> (
        s"UPDATE system_tablegroup_lang SET name = ? WHERE id = ? AND langtag = ?",
        Seq(name, tableGroupId, langtag)
        ))
    case (m, DescriptionOnly(langtag, description)) =>
      m + (langtag -> (
        s"UPDATE system_tablegroup_lang SET description = ? WHERE id = ? AND langtag = ?",
        Seq(description, tableGroupId, langtag)
        ))
    case (m, NameAndDescription(langtag, name, description)) =>
      m + (langtag -> (
        s"UPDATE system_tablegroup_lang SET name = ?, description = ? WHERE id = ? AND langtag = ?",
        Seq(name, description, tableGroupId, langtag)
        ))
  }
}

class ColumnDisplayInfos(tableId: TableId, columnId: ColumnId, override val entries: Seq[DisplayInfo]) extends DisplayInfos {

  override def nonEmpty: Boolean = entries.nonEmpty

  def statement: String = {
    s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
        |VALUES ${entries.map(_ => s"(?, ?, ?, ?, ?)").mkString(", ")}""".stripMargin
  }

  def binds: Seq[Any] = entries.flatMap {
    di => List(tableId, columnId, di.langtag, di.optionalName.orNull, di.optionalDescription.orNull)
  }

}

class TableDisplayInfos(tableId: TableId, override val entries: Seq[DisplayInfo]) extends DisplayInfos {

  override def nonEmpty: Boolean = entries.nonEmpty

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

  private def getFieldNames(json: JsonObject, field: String): Seq[String] = {
    import scala.collection.JavaConverters._

    Option(json.getJsonObject(field))
      .map({
        json =>
          if (json.fieldNames().size() > 0) {
            json.fieldNames().asScala.toSeq
          } else {
            Seq.empty
          }
      })
      .getOrElse(Seq.empty)
  }

  def allInfos(json: JsonObject): Seq[DisplayInfo] = {

    val nameLangtags = getFieldNames(json, "displayName")
    val descriptionLangtags = getFieldNames(json, "description")

    val both = nameLangtags.intersect(descriptionLangtags) map { lang =>
      NameAndDescription(lang,
        json.getJsonObject("displayName").getString(lang),
        json.getJsonObject("description").getString(lang)
      )
    }

    val nameOnly = nameLangtags.diff(descriptionLangtags) map { lang =>
      NameOnly(lang, json.getJsonObject("displayName").getString(lang))
    }

    val descOnly = descriptionLangtags.diff(nameLangtags) map { lang =>
      DescriptionOnly(lang, json.getJsonObject("description").getString(lang))
    }

    both.toList ::: nameOnly.toList ::: descOnly.toList
  }

  def fromString(langtag: String, displayName: String, description: String): DisplayInfo = (Option(displayName), Option(description)) match {
    case (Some(_), None) => NameOnly(langtag, displayName)
    case (None, Some(_)) => DescriptionOnly(langtag, description)
    case (Some(_), Some(_)) => NameAndDescription(langtag, displayName, description)
    case (None, None) => throw new IllegalArgumentException("Either displayName or description must be defined.")
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
