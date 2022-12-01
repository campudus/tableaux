package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.database.{LanguageNeutral, MultiCountry, MultiLanguage}

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

    val conditionLangtag: ConditionOption =
      Option(jsonObject.getString("langtag"))
        .map(langtags => ConditionLangtag(Json.obj("langtag" -> langtags)))
        .getOrElse(NoneCondition)

    new ConditionContainer(conditionTable, conditionColumn, conditionLangtag)
  }
}

case class ConditionContainer(
    conditionTable: ConditionOption,
    conditionColumn: ConditionOption,
    conditionLangtag: ConditionOption
) extends LazyLogging {

  def isMatching(action: Action, objects: ComparisonObjects): Boolean = {
    def tableActions = Seq(
      ViewTable,
      CreateRow,
      DeleteRow,
      EditCellAnnotation,
      EditRowAnnotation,
      EditTableDisplayProperty,
      EditTableStructureProperty,
      ViewHiddenTable,
      CreateColumn,
      DeleteColumn
    )
    def isTableAction(action: Action): Boolean = tableActions.contains(action)

    def columnActions = Seq(
      ViewColumn,
      EditColumnDisplayProperty,
      EditColumnStructureProperty
    )
    def isColumnAction(action: Action): Boolean = columnActions.contains(action)

    def isLangtagAction(action: Action): Boolean = action == EditCellValue

    def globalActions = Seq(
      CreateTable,
      DeleteTable,
      CreateMedia,
      EditMedia,
      DeleteMedia,
      CreateTableGroup,
      EditTableGroup,
      DeleteTableGroup,
      CreateService,
      DeleteService,
      ViewService,
      EditServiceDisplayProperty,
      EditServiceStructureProperty,
      EditSystem
    )
    def isGlobalAction(action: Action): Boolean = globalActions.contains(action)

    if (isLangtagAction(action)) {
      logger.debug(
        s"try matching on conditionTable: $conditionTable conditionColumn $conditionColumn conditionLangtag $conditionLangtag"
      )
      conditionTable.isMatching(objects) &&
      conditionColumn.isMatching(objects) &&
      conditionLangtag.isMatching(objects)
    } else if (isColumnAction(action)) {
      logger.debug(
        s"try matching on conditionTable: $conditionTable conditionColumn $conditionColumn"
      )
      conditionTable.isMatching(objects) &&
      conditionColumn.isMatching(objects)
    } else if (isTableAction(action)) {
      logger.debug(
        s"try matching on conditionTable: $conditionTable"
      )
      conditionTable.isMatching(objects)
    } else {
      conditionColumn.isMatching(objects)
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
