package com.campudus.tableaux.router.auth.permission

import org.vertx.scala.core.json.{Json, JsonObject, _}

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
) {

  def isMatching(objects: ComparisonObjects): Boolean =
    conditionTable.isMatching(objects) &&
      conditionColumn.isMatching(objects) &&
      conditionLangtag.isMatching(objects)
}

abstract class ConditionOption(jsonObject: JsonObject) {
  val conditionMap: Map[String, String] = toMap(jsonObject)

  protected def toMap(jsonObject: JsonObject): Map[String, String] = {
    jsonObject.asMap.toMap.asInstanceOf[Map[String, String]]
  }

  def isMatching(objects: ComparisonObjects): Boolean = false
}

case class ConditionTable(jsonObject: JsonObject) extends ConditionOption(jsonObject) {

  override def isMatching(subjects: ComparisonObjects): Boolean = {
    subjects.tableOpt.exists(table =>
      conditionMap.forall({
        case (property, regex) => {

          // TODO possibly not the best idea to stringify every property and match with regex!?
          property match {
            case "id" => table.id.toString.matches(regex)
            case "name" => table.name.matches(regex)
            case "hidden" => table.hidden.toString.matches(regex)
            case "tableType" => table.tableType.NAME.matches(regex)
            case "tableGroup" => table.tableGroup.exists(_.id.toString.matches(regex))
            case _ => false
          }
        }
      }))
  }
}

case class ConditionColumn(jsonObject: JsonObject) extends ConditionOption(jsonObject)

// TODO implement regex validation for all possible fields
//  - description
//  - displayName
//  - id
//  - identifier
//  - kind
//  - multilanguage
//  - name
//  - ordering

case class ConditionLangtag(jsonObject: JsonObject) extends ConditionOption(jsonObject)
// TODO implement regex validation

case object NoneCondition extends ConditionOption(Json.emptyObj()) {
  override def isMatching(subjects: ComparisonObjects): Boolean = true
}
