package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.model.structure.{ColumnModel, TableGroupModel, TableModel}
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}

object StructureModel {
  def apply(connection: DatabaseConnection): StructureModel = {
    new StructureModel(connection)
  }
}

class StructureModel(override protected[this] val connection: DatabaseConnection) extends DatabaseQuery {
  val tableStruc = new TableModel(connection)
  val columnStruc = new ColumnModel(connection)
  val tableGroupStruc = new TableGroupModel(connection)
}
