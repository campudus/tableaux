package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.model.structure.{ColumnModel, TableModel}
import com.campudus.tableaux.database.{DatabaseQuery, DatabaseConnection}

object StructureModel {
  def apply(connection: DatabaseConnection): StructureModel = {
    new StructureModel(connection)
  }
}

class StructureModel(override protected[this] val connection: DatabaseConnection) extends DatabaseQuery {
  val tableStruc = new TableModel(connection)
  val columnStruc = new ColumnModel(connection)
}
