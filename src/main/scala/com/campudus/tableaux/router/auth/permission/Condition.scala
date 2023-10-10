package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.database.{LanguageNeutral, MultiCountry, MultiLanguage}
import com.campudus.tableaux.database.domain.Row
import com.campudus.tableaux.database.domain.RowPermissions

import org.vertx.scala.core.json.{Json, JsonObject, _}

import scala.collection.JavaConverters._

import com.typesafe.scalalogging.LazyLogging

object ConditionContainer {

  def apply(jsonObjectOrNull: JsonObject): ConditionContainer = {

    val jsonObject: JsonObject = Option(jsonObjectOrNull).getOrElse(Json.emptyObj())

    val conditionTable: ConditionOption =
      Option(jsonObject.getJsonObject("table")).map(ConditionTable).getOrElse(NoneCondition)

    val conditionColumn: ConditionOption =
      Option(jsonObject.getJsonObject("column")).map(ConditionColumn).getOrElse(NoneCondition)

    val conditionRow: ConditionOption =
      Option(jsonObject.getJsonObject("row")).map(ConditionRow).getOrElse(NoneCondition)

    val conditionLangtag: ConditionOption =
      Option(jsonObject.getString("langtag"))
        .map(langtags => ConditionLangtag(Json.obj("langtag" -> langtags)))
        .getOrElse(NoneCondition)

    new ConditionContainer(conditionTable, conditionColumn, conditionLangtag, conditionRow)
  }
}

case class ConditionContainer(
    conditionTable: ConditionOption,
    conditionColumn: ConditionOption,
    conditionLangtag: ConditionOption,
    conditionRow: ConditionOption
) extends LazyLogging {

  def isMatching(action: Action, objects: ComparisonObjects): Boolean = {

    action match {
      case EditCellValue => {
        logger.debug(
          s"matching on action: $action conditionTable: $conditionTable conditionColumn $conditionColumn conditionLangtag $conditionLangtag"
        )
        conditionTable.isMatching(objects) &&
        conditionColumn.isMatching(objects) &&
        conditionLangtag.isMatching(objects)
      }
      case ViewCellValue | DeleteColumn | ViewColumn
          | EditColumnDisplayProperty | EditColumnStructureProperty => {
        logger.debug(s"matching on action: $action conditionTable: $conditionTable conditionColumn $conditionColumn")
        conditionTable.isMatching(objects) &&
        conditionColumn.isMatching(objects)
      }
      case ViewTable | DeleteTable | CreateRow | DeleteRow | EditCellAnnotation
          | EditRowAnnotation | EditTableDisplayProperty | EditTableStructureProperty
          | ViewHiddenTable | CreateColumn => {
        logger.debug(s"matching on action: $action conditionTable: $conditionTable")
        conditionTable.isMatching(objects)
      }
      case ViewRow => {
        logger.debug(s"matching on action: $action conditionRow: $conditionRow")
        conditionRow.isMatching(objects)
      }
      case CreateTable | CreateMedia | EditMedia
          | DeleteMedia | CreateTableGroup | EditTableGroup | DeleteTableGroup
          | CreateService | DeleteService | ViewService | EditServiceDisplayProperty
          | EditServiceStructureProperty | EditSystem => {
        // global actions are already filtered by filterPermissions
        true
      }
      case _ => throw new IllegalArgumentException(s"Unknown action")
    }
  }
}

abstract class ConditionOption(jsonObject: JsonObject) extends LazyLogging {
  val conditionMap: Map[String, String] = toMap(jsonObject)

  protected def toMap(jsonObject: JsonObject): Map[String, String] = {
    jsonObject.asMap.toMap.asInstanceOf[Map[String, String]]
  }

  def isMatching(objects: ComparisonObjects): Boolean = false
}

case class ConditionTable(jsonObject: JsonObject) extends ConditionOption(jsonObject) {

  override def isMatching(objects: ComparisonObjects): Boolean = {
    objects.tableOpt match {
      case Some(table) =>
        conditionMap.forall({
          case (property, regex) =>
            property match {
              case "id" => table.id.toString.matches(regex)
              case "name" => table.name.matches(regex)
              case "hidden" => table.hidden.toString.matches(regex)
              case "tableType" => table.tableType.NAME.matches(regex)
              case "tableGroup" => table.tableGroup.exists(_.id.toString.matches(regex))
              case _ => false
            }
        })
      case None => false
    }
  }
}

case class ConditionColumn(jsonObject: JsonObject) extends ConditionOption(jsonObject) {

  override def isMatching(objects: ComparisonObjects): Boolean = {

    objects.columnOpt match {
      case Some(column) =>
        conditionMap.forall({
          case (property, regex) =>
            property match {
              case "id" => column.id.toString.matches(regex)
              case "name" => column.name.matches(regex)
              case "identifier" => column.identifier.toString.matches(regex)
              case "kind" => column.kind.toString.matches(regex)
              case "multilanguage" => {
                val isMultilanguage: Boolean = column.languageType != LanguageNeutral
                isMultilanguage.toString.matches(regex)
              }
              case _ => false
            }
        })
      case None => false
    }
  }
}

case class ConditionRow(jsonObject: JsonObject) extends ConditionOption(jsonObject) {

  def checkCondition(rowPermissionsOpt: Option[RowPermissions]): Boolean = {
    conditionMap.forall({
      case (property, regex) =>
        property match {
          case "permissions" => {
            rowPermissionsOpt match {
              case Some(rowPermissions) => {
                rowPermissions.value.exists(_.matches(regex))
              }
              case None => false
            }
          }
          case _ => false
        }
    })

  }

  override def isMatching(objects: ComparisonObjects): Boolean = {
    (objects.rowOpt, objects.rowPermissionsOpt) match {
      case (Some(row: Row), _) =>
        Option(row.rowPermissions) match {
          case Some(rp) if rp.value.size == 0 => false
          case None => false
          case Some(rp) => checkCondition(Some(rp))
        }
      case (_, Some(rowPermissions: RowPermissions)) => checkCondition(Some(rowPermissions))
      case (_, _) => false
    }
  }
}

case class ConditionLangtag(jsonObject: JsonObject) extends ConditionOption(jsonObject) {

  override def isMatching(objects: ComparisonObjects): Boolean = {

    // At this point, the value for the column type must already have been checked. -> checkValueTypeForColumn
    objects.columnOpt match {
      case Some(column) =>
        column.languageType match {

          case MultiLanguage | MultiCountry(_) =>
            objects.valueOpt match {
              case Some(json: JsonObject) => {
                val regex: String = conditionMap.getOrElse("langtag", ".*")

                json
                  .fieldNames()
                  .asScala
                  .forall(langtag => {
                    logger.debug(s"Matching langtag: $langtag -> ${langtag.matches(regex)}")
                    langtag.matches(regex)
                  })
              }
              case _ => true
            }

          case LanguageNeutral => true
        }

      case None => false
    }
  }

}

case object NoneCondition extends ConditionOption(Json.emptyObj()) {
  override def isMatching(objects: ComparisonObjects): Boolean = true
}
