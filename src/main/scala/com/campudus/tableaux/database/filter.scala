package com.campudus.tableaux.database

import com.campudus.tableaux.InvalidRequestException
import com.campudus.tableaux.database.model.TableauxModel.ColumnId

sealed trait FilterOperation {
  val name: String
}

object FilterOperation {
  def apply(operation: String): FilterOperation = {
    operation.toLowerCase() match {
      case ContainsNotOperation.name => ContainsNotOperation
      case ContainsOperation.name => ContainsOperation

      case IsNotOperation.name => IsNotOperation
      case IsOperation.name => IsOperation

      case NotEmptyOperation.name => NotEmptyOperation
      case EmptyOperation.name => EmptyOperation
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
    val splitted = filter.split(",")
    val filterItems = if (splitted.size % 3 != 0) {
      throw InvalidRequestException("Filter is invalid.")
    } else {
      splitted.sliding(3, 3).map({
        filterItem =>
          val columnId = filterItem(0).toLong
          val operation = FilterOperation(filterItem(1))
          val filterValue = filterItem(2)

          FilterItem(columnId, operation, filterValue)
      }).toList
    }

    Filter(filterItems)
  }
}

case class Filter(val filters: List[FilterItem])