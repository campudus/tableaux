package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel.TableId

import org.vertx.scala.core.json.{Json, JsonObject}

case class TableGroup(id: TableId, displayInfos: Seq[DisplayInfo]) extends DomainObject {

  override def getJson: JsonObject = {
    val result = Json.obj(
      "id" -> id,
      "displayName" -> Json.obj(),
      "description" -> Json.obj()
    )

    displayInfos.foreach { di =>
      {
        di.optionalName.map(name => {
          result.mergeIn(
            Json.obj("displayName" -> result.getJsonObject("displayName").mergeIn(Json.obj(di.langtag -> name)))
          )
        })
        di.optionalDescription.map(desc => {
          result.mergeIn(
            Json.obj("description" -> result.getJsonObject("description").mergeIn(Json.obj(di.langtag -> desc)))
          )
        })
      }
    }

    result
  }
}

case class TableGroupSeq(tableGroups: Seq[TableGroup]) extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj(
      "tableGroups" -> tableGroups.map(_.getJson)
    )
  }
}
