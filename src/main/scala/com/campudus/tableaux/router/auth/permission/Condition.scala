package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.database.MultiLanguage
import com.campudus.tableaux.database.domain.ColumnType
import com.typesafe.scalalogging.LazyLogging
import org.vertx.scala.core
import org.vertx.scala.core.json
import org.vertx.scala.core.json.{Json, JsonObject, _}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success}

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

  def isMatching(objects: ComparisonObjects, withLangtagCondition: Boolean): Boolean = {
    logger.debug(
      s"try matching on conditionTable: $conditionTable conditionColumn $conditionColumn conditionLangtag $conditionLangtag with objects $objects")

    if (withLangtagCondition) {
      conditionTable.isMatching(objects) &&
      conditionColumn.isMatching(objects) &&
      conditionLangtag.isMatching(objects)
    } else {
      conditionTable.isMatching(objects) &&
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
            // TODO possibly not the best idea to stringify every property and match with regex!?
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
              case "multilanguage" => column.languageType.toString.matches(regex)
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
        if (column.languageType == MultiLanguage) {

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
        } else {
          true
        }

      case None => false
    }
  }

}

case object NoneCondition extends ConditionOption(Json.emptyObj()) {
  override def isMatching(objects: ComparisonObjects): Boolean = true
}
