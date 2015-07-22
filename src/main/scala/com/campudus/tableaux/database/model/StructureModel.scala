package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.model.tableaux.{ColumnStructure, TableStructure}
import com.campudus.tableaux.database.{DatabaseQuery, DatabaseConnection}

object StructureModel {
  def apply(connection: DatabaseConnection): StructureModel = {
    new StructureModel(connection)
  }
}

class StructureModel(override protected[this] val connection: DatabaseConnection) extends DatabaseQuery {
  val tableStruc = new TableStructure(connection)
  val columnStruc = new ColumnStructure(connection)

  //TODO meeeeh
  lazy val tableauxModel = TableauxModel(connection)
}
