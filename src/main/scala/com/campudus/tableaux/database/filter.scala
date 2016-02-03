package com.campudus.tableaux.database

import com.campudus.tableaux.database.model.TableauxModel.ColumnId

sealed trait FilterOperation {
  val name: String
}

object FilterOperation {
  def apply(operation: String): FilterOperation = {
    operation.toLowerCase() match {
      case ContainsOperation.name => ContainsOperation
      case IsOperation.name => IsOperation
    }
  }
}

case object ContainsNotOperation extends FilterOperation {
  override val name = "!contains"
}

case object ContainsOperation extends FilterOperation {
  override val name = "contains"
}

case object IsNotOperation extends FilterOperation {
  override val name = "!is"
}

case object IsOperation extends FilterOperation {
  override val name = "is"
}

case object EmptyOperation extends FilterOperation {
  override val name = "empty"
}

case object NotEmptyOperation extends FilterOperation {
  override val name = "!empty"
}

case class FilterItem(columnId: ColumnId, operation: FilterOperation, filterValue: String) {
  override def toString(): String = s"[$columnId ${operation.name} $filterValue]"
}

object Filter {
  def apply(filter: String): Filter = {
    import com.campudus.tableaux.ArgumentChecker._

    for {
      splitted <- isSize(filter.split(","))
    }

    filter.split(",")

    Filter(List.empty[FilterItem])
  }
}

case class Filter(filters: List[FilterItem])