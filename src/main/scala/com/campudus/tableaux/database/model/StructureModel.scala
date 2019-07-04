package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.model.structure.{CachedColumnModel, TableGroupModel, TableModel}
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import org.vertx.scala.core.json.JsonObject

object StructureModel {

  def apply(connection: DatabaseConnection): StructureModel = {
    new StructureModel(connection)
  }
}

class StructureModel(override protected[this] val connection: DatabaseConnection) extends DatabaseQuery {
  val tableModel: TableModel = new TableModel(connection)

  val columnStruc =
    new CachedColumnModel(connection.vertx.getOrCreateContext().config().getOrElse(new JsonObject()), connection)
  val tableGroupStruc = new TableGroupModel(connection)
}
