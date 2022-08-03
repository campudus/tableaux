package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.model.structure.{CachedColumnModel, TableGroupModel, TableModel}
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.router.auth.permission.RoleModel
import org.vertx.scala.core.json.Json

object StructureModel {

  def apply(connection: DatabaseConnection)(
      implicit roleModel: RoleModel
  ): StructureModel = {
    new StructureModel(connection)
  }
}

class StructureModel(override protected[this] val connection: DatabaseConnection)(
    implicit roleModel: RoleModel
) extends DatabaseQuery {
  val tableStruc = new TableModel(connection)

  val columnStruc =
    new CachedColumnModel(connection.vertx.getOrCreateContext().config().getOrElse(Json.obj()), connection)
  val tableGroupStruc = new TableGroupModel(connection)
}
