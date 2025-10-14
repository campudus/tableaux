package com.campudus.tableaux.helper

import com.campudus.tableaux.NotImplementedException
import com.campudus.tableaux.database.domain.Table
import com.campudus.tableaux.database.domain.TableType
import com.campudus.tableaux.database.domain.UnionTable

object UnionTableHelper {

  val message = "Operation not implemented for table of type union"

  def notImplemented(table: Table) = {
    table.tableType match {
      case UnionTable => throw new NotImplementedException(message)
      case _ =>
    }
  }
}
