package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.domain.DisplayInfos._
import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

object DisplayInfo {

  def unapply(displayInfo: DisplayInfo): Option[(Langtag, Option[String], Option[String])] = {
    Some(displayInfo.langtag, displayInfo.optionalName, displayInfo.optionalDescription)
  }
}

sealed trait DisplayInfo {
  val langtag: Langtag
  val optionalName: Option[String]
  val optionalDescription: Option[String]
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

object DisplayInfos {
  type Langtag = String

  private def getFieldNames(json: JsonObject, field: String): Seq[String] = {
    import scala.collection.JavaConverters._

    Option(json.getJsonObject(field))
      .map(json => {
        if (json.fieldNames().size() > 0) {
          json.fieldNames().asScala.toSeq
        } else {
          Seq.empty
        }
      })
      .getOrElse(Seq.empty)
  }

  def fromJson(json: JsonObject): Seq[DisplayInfo] = {

    val nameLangtags = getFieldNames(json, "displayName")
    val descriptionLangtags = getFieldNames(json, "description")

    val both = nameLangtags
      .intersect(descriptionLangtags)
      .map(lang => {
        NameAndDescription(lang,
                           json.getJsonObject("displayName").getString(lang),
                           json.getJsonObject("description").getString(lang))
      })

    val nameOnly = nameLangtags
      .diff(descriptionLangtags)
      .map(lang => NameOnly(lang, json.getJsonObject("displayName").getString(lang)))

    val descOnly = descriptionLangtags
      .diff(nameLangtags)
      .map(lang => DescriptionOnly(lang, json.getJsonObject("description").getString(lang)))

    both.toList ::: nameOnly.toList ::: descOnly.toList
  }

  def fromString(langtag: String, displayName: String, description: String): DisplayInfo = {
    (Option(displayName), Option(description)) match {
      case (Some(_), None) => NameOnly(langtag, displayName)
      case (None, Some(_)) => DescriptionOnly(langtag, description)
      case (Some(_), Some(_)) => NameAndDescription(langtag, displayName, description)
      case (None, None) => throw new IllegalArgumentException("Either displayName or description must be defined.")
    }
  }
}

sealed trait DisplayInfos {
  val entries: Seq[DisplayInfo]

  def nonEmpty: Boolean = entries.nonEmpty

  def createSql: (String, Seq[Any])

  protected def createSql(table: String, idColumn: String, idValue: Long): (String, Seq[Any]) = {
    val statement =
      s"INSERT INTO $table ($idColumn, langtag, name, description) VALUES ${entries
        .map(_ => "(?, ?, ?, ?)")
        .mkString(", ")}"
    val binds = entries.flatMap(di => List(idValue, di.langtag, di.optionalName.orNull, di.optionalDescription.orNull))

    (statement, binds)
  }

  def insertSql: Map[Langtag, (String, Seq[Any])]

  protected def insertSql(table: String, idColumn: String, idValue: Long): Map[Langtag, (String, Seq[Any])] = {
    entries.foldLeft(Map.empty[Langtag, (String, Seq[Any])]) {
      case (resultMap, DisplayInfo(langtag, nameOpt, descriptionOpt)) =>
        val nameAndDesc = nameOpt.map(_ => "name").toList ::: descriptionOpt.map(_ => "description").toList
        val statement =
          s"INSERT INTO $table (${nameAndDesc.mkString(", ")}, $idColumn, langtag) VALUES (${nameAndDesc
            .map(_ => "?")
            .mkString(", ")}, ?, ?)"
        val binds = nameOpt.toList ::: descriptionOpt.toList ::: List(idValue, langtag)

        resultMap + (langtag -> (statement, binds))
    }
  }

  def updateSql: Map[Langtag, (String, Seq[Any])]

  protected def updateSql(table: String, idColumn: String, idValue: Long): Map[Langtag, (String, Seq[Any])] = {
    entries.foldLeft(Map.empty[Langtag, (String, Seq[Any])]) {
      case (resultMap, DisplayInfo(langtag, nameOpt, descriptionOpt)) =>
        val nameAndDesc = nameOpt.map(_ => "name = ?").toList ::: descriptionOpt.map(_ => "description = ?").toList
        val statement = s"UPDATE $table SET ${nameAndDesc.mkString(", ")} WHERE $idColumn = ? AND langtag = ?"
        val binds = nameOpt.toList ::: descriptionOpt.toList ::: List(idValue, langtag)

        resultMap + (langtag -> (statement, binds))
    }
  }
}

case class TableGroupDisplayInfos(tableGroupId: TableGroupId, override val entries: Seq[DisplayInfo])
    extends DisplayInfos {
  val table: String = "system_tablegroup_lang"
  val idColumn: String = "id"
  val idValue: TableGroupId = tableGroupId

  override def createSql: (String, Seq[Any]) = createSql(table, idColumn, idValue)

  override def insertSql: Map[Langtag, (String, Seq[Any])] = insertSql(table, idColumn, idValue)

  override def updateSql: Map[Langtag, (String, Seq[Any])] = updateSql(table, idColumn, idValue)
}

case class TableDisplayInfos(tableId: TableId, override val entries: Seq[DisplayInfo]) extends DisplayInfos {
  val table: String = "system_table_lang"
  val idColumn: String = "table_id"
  val idValue: TableId = tableId

  override def createSql: (String, Seq[Any]) = createSql(table, idColumn, idValue)

  override def insertSql: Map[Langtag, (String, Seq[Any])] = insertSql(table, idColumn, idValue)

  override def updateSql: Map[Langtag, (String, Seq[Any])] = updateSql(table, idColumn, idValue)
}

case class ColumnDisplayInfos(tableId: TableId, columnId: ColumnId, override val entries: Seq[DisplayInfo])
    extends DisplayInfos {

  override def createSql: (String, Seq[Any]) = {
    val statement =
      s"INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description) VALUES ${entries
        .map(_ => "(?, ?, ?, ?, ?)")
        .mkString(", ")}"
    val binds =
      entries.flatMap(di => List(tableId, columnId, di.langtag, di.optionalName.orNull, di.optionalDescription.orNull))

    (statement, binds)
  }

  override def insertSql: Map[Langtag, (String, Seq[Any])] = {
    entries.foldLeft(Map.empty[Langtag, (String, Seq[Any])]) {
      case (resultMap, DisplayInfo(langtag, nameOpt, descriptionOpt)) =>
        val nameAndDesc = nameOpt.map(_ => "name").toList ::: descriptionOpt.map(_ => "description").toList
        val statement =
          s"INSERT INTO system_columns_lang (${nameAndDesc
            .mkString(", ")}, table_id, column_id, langtag) VALUES (${nameAndDesc.map(_ => "?").mkString(", ")}, ?, ?, ?)"
        val binds = nameOpt.toList ::: descriptionOpt.toList ::: List(tableId, columnId, langtag)

        resultMap + (langtag -> (statement, binds))
    }
  }

  override def updateSql: Map[Langtag, (String, Seq[Any])] = {
    entries.foldLeft(Map.empty[Langtag, (String, Seq[Any])]) {
      case (resultMap, DisplayInfo(langtag, nameOpt, descriptionOpt)) =>
        val nameAndDesc = nameOpt.map(_ => "name = ?").toList ::: descriptionOpt.map(_ => "description = ?").toList
        val statement =
          s"UPDATE system_columns_lang SET ${nameAndDesc
            .mkString(", ")} WHERE table_id = ? AND column_id = ? AND langtag = ?"
        val binds = nameOpt.toList ::: descriptionOpt.toList ::: List(tableId, columnId, langtag)

        resultMap + (langtag -> (statement, binds))
    }
  }
}
