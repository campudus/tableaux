package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.helper.HelperFunctions
import HelperFunctions._
import com.campudus.tableaux.{InvalidJsonException, ArgumentChecker}
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.tableaux.{TableStructure, RowStructure, ColumnStructure, CellStructure}
import org.vertx.scala.core.json._

import scala.concurrent.Future
import scala.util.Try

object TableauxModel {
  type TableId = Long
  type ColumnId = Long
  type RowId = Long

  type Ordering = Long

  def apply(connection: DatabaseConnection): TableauxModel = {
    new TableauxModel(connection)
  }
}

class TableauxModel(override protected[this] val connection: DatabaseConnection) extends DatabaseQuery {

  import TableauxModel._

  val tableStruc = new TableStructure(connection)
  val columnStruc = new ColumnStructure(connection)
  val cellStruc = new CellStructure(connection)
  val rowStruc = new RowStructure(connection)

  val attachmentModel = AttachmentModel(connection)

  def createTable(name: String): Future[Table] = for {
    id <- tableStruc.create(name)
  } yield Table(id, name)

  def createCompleteTable(name: String, columnsNameAndType: Seq[CreateColumn], rowsValues: Seq[Seq[_]]): Future[CompleteTable] = for {
    table <- createTable(name)
    columnIds <- addColumns(table.id, columnsNameAndType) map { colSeq => colSeq.columns map { col => col.id } }
    rowsWithColumnIdAndValue <- Future.successful {
      if (rowsValues.isEmpty) {
        Seq()
      } else {
        rowsValues map {
          columnIds.zip(_)
        }
      }
    }
    _ <- addFullRows(table.id, rowsWithColumnIdAndValue)
    completeTable <- getCompleteTable(table.id)
  } yield completeTable

  def deleteTable(tableId: TableId): Future[EmptyObject] = for {
    _ <- tableStruc.delete(tableId)
  } yield EmptyObject()

  def deleteRow(tableId: TableId, rowId: RowId): Future[EmptyObject] = for {
    _ <- rowStruc.delete(tableId, rowId)
  } yield EmptyObject()

  def addColumns(tableId: TableId, columns: Seq[CreateColumn]): Future[ColumnSeq] = for {
    cols <- serialiseFutures(columns) {
      case CreateSimpleColumn(name, ordering, kind, languageType) =>
        addValueColumn(tableId, name, kind, ordering, languageType)

      case CreateLinkColumn(name, ordering, linkConnection) =>
        addLinkColumn(tableId, name, linkConnection, ordering)

      case CreateAttachmentColumn(name, ordering) =>
        addAttachmentColumn(tableId, name, ordering)
    }
  } yield ColumnSeq(cols)

  def addValueColumn(tableId: TableId, name: String, columnType: TableauxDbType, ordering: Option[Ordering], languageType: LanguageType): Future[ColumnType[_]] = for {
    table <- getTable(tableId)
    (id, ordering) <- columnStruc.insert(table.id, columnType, name, ordering, languageType)
  } yield Mapper(languageType, columnType).apply(table, id, name, ordering)

  def addLinkColumn(tableId: TableId, name: String, linkConnection: LinkConnection, ordering: Option[Ordering]): Future[LinkColumn[_]] = for {
    table <- getTable(tableId)
    toCol <- getColumn(linkConnection.toTableId, linkConnection.toColumnId).asInstanceOf[Future[SimpleValueColumn[_]]]
    (id, ordering) <- columnStruc.insertLink(tableId, name, linkConnection.fromColumnId, toCol.table.id, toCol.id, ordering)
  } yield LinkColumn(table, id, toCol, name, ordering)

  def addAttachmentColumn(tableId: TableId, name: String, ordering: Option[Ordering]): Future[AttachmentColumn] = for {
    table <- getTable(tableId)
    (id, ordering) <- columnStruc.insertAttachment(table.id, name, ordering)
  } yield AttachmentColumn(table, id, name, ordering)

  def removeColumn(tableId: TableId, columnId: ColumnId): Future[EmptyObject] = for {
    _ <- columnStruc.delete(tableId, columnId)
  } yield EmptyObject()

  def addRow(tableId: TableId): Future[Row] = for {
    table <- getTable(tableId)
    id <- rowStruc.createEmpty(tableId)
  } yield Row(table, id, Seq.empty)

  def addFullRows(tableId: TableId, rows: Seq[Seq[(ColumnId, Any)]]): Future[RowSeq] = for {
    table <- getTable(tableId)
    columns <- getAllColumns(table)
    ids <- serialiseFutures(rows) {
      row =>
        // replace ColumnId with ColumnType
        val mappedRow = row.map { case (columnId, value) => (columns.find(_.id == columnId).get, value) }

        val singleValues = mappedRow.filter {
          case (_: MultiLanguageColumn[_], _) => false
          case (_, _) => true
        }

        val multiValues = mappedRow.filter {
          case (_: MultiLanguageColumn[_], _) => true
          case (_, _) => false
        } map { case (column, value) =>
          (column, toTupleSeq(value.asInstanceOf[JsonObject]))
        }

        for {
          rowId <- if (singleValues.isEmpty) {
            rowStruc.createEmpty(table.id)
          } else {
            rowStruc.createFull(table.id, singleValues)
          }

          _ <- if (multiValues.nonEmpty) {
            rowStruc.createTranslations(table.id, rowId, multiValues)
          } else {
            Future()
          }
        } yield rowId
    }

    row <- serialiseFutures(ids)(getRow(table.id, _))
  } yield RowSeq(row)

  def updateValue[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = for {
  // TODO column should be passed on to insert methods
    column <- getColumn(tableId, columnId)
    cell <- column match {
      case attachmentColumn: AttachmentColumn => updateAttachment(tableId, columnId, rowId, value)
      case _ => insertValue(tableId, columnId, rowId, value)
    }
  } yield cell

  def insertValue[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = for {
    // TODO column should be passed on to insert methods
    column <- getColumn(tableId, columnId)
    cell <- column match {
      case column: AttachmentColumn => insertAttachment(tableId, columnId, rowId, value)
      case column: LinkColumn[_] => handleLinkValues(tableId, columnId, rowId, value)
      case column: MultiLanguageColumn[_] => insertMultilanguageValues(tableId, columnId, rowId, value.asInstanceOf[JsonObject])
      case _ => insertSimpleValue(tableId, columnId, rowId, value)
    }
  } yield cell

  def insertAttachment[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[AttachmentFile]] = {
    // add attachment
    handleAttachment(tableId, columnId, rowId, value, attachmentModel.add)
  }

  def updateAttachment[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[AttachmentFile]] = {
    // update attachment order
    handleAttachment(tableId, columnId, rowId, value, attachmentModel.update)
  }

  private def handleAttachment[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A, fn: Attachment => Future[AttachmentFile]): Future[Cell[AttachmentFile]] = {
    import ArgumentChecker._

    Try(value.asInstanceOf[JsonObject]) map { v =>
      val uuid = notNull(v.getString("uuid"), "uuid").map(UUID.fromString).get
      val ordering: Option[Ordering] = {
        if (v.containsField("ordering")) {
          Option(v.getLong("ordering"))
        } else {
          None
        }
      }

      for {
        column <- getColumn(tableId, columnId)
        file <- fn(Attachment(tableId, columnId, rowId, uuid, ordering))
      } yield Cell(column.asInstanceOf[AttachmentColumn], rowId, file)
    } getOrElse {
      Future.failed(InvalidJsonException(s"A attachment value must be a UUID but got $value", "attachment-value"))
    }
  }

  private def insertMultilanguageValues[A <: JsonObject](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[Any]] = {
    for {
      column <- getColumn(tableId, columnId)
      _ <- cellStruc.updateTranslations(column.table.id, column.id, rowId, toTupleSeq(value))
    } yield Cell(column, rowId, value)
  }

  private def insertSimpleValue[A, B <: ColumnType[A]](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[A]] = for {
    column <- getColumn(tableId, columnId)
    _ <- cellStruc.update(tableId, columnId, rowId, value)
  } yield Cell(column.asInstanceOf[B], rowId, value)

  private def handleLinkValues[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[Link[Any]]] = {
    Try(value.asInstanceOf[JsonObject]).flatMap { v =>
      import ArgumentChecker._
      import collection.JavaConverters._

      for {
        from <- Try(checked(hasNumber("from", v)).longValue())
        tos <- Try(Left(checked(hasNumber("to", v)).longValue())).orElse(
          Try(Right(checked(hasArray("values", v)).asScala.map(_.asInstanceOf[Number].longValue()).toSeq)))
      } yield {
        tos match {
          case Left(to) => addLinkValue(tableId, columnId, rowId, from, to)
          case Right(toList) => insertLinkValues(tableId, columnId, rowId, from, toList)
        }
      }
    } getOrElse {
      Future.failed(InvalidJsonException(s"A link column expects a JSON object with from and to values, but got $value", "link-value"))
    }
  }

  private def insertLinkValues(tableId: TableId, columnId: ColumnId, rowId: RowId, from: RowId, tos: Seq[RowId]): Future[Cell[Link[Any]]] = for {
    linkColumn <- getColumn(tableId, columnId).asInstanceOf[Future[LinkColumn[Any]]]
    _ <- cellStruc.putLinks(linkColumn.table.id, linkColumn.id, from, tos)
    v <- cellStruc.getLinkValues(linkColumn.table.id, linkColumn.id, rowId, linkColumn.to.table.id, linkColumn.to.id)
  } yield Cell(linkColumn, rowId, Link(linkColumn.to.id, v))

  def addLinkValue(tableId: TableId, columnId: ColumnId, rowId: RowId, from: RowId, to: RowId): Future[Cell[Link[Any]]] = for {
    linkColumn <- getColumn(tableId, columnId).asInstanceOf[Future[LinkColumn[Any]]]
    _ <- cellStruc.updateLink(linkColumn.table.id, linkColumn.id, from, to)
    v <- cellStruc.getLinkValues(linkColumn.table.id, linkColumn.id, rowId, linkColumn.to.table.id, linkColumn.to.id)
  } yield Cell(linkColumn, rowId, Link(to, v))

  def getAllTables(): Future[TableSeq] = {
    for {
      seqOfTableInformation <- tableStruc.retrieveAll()
    } yield {
      TableSeq(seqOfTableInformation map { case (id, name) => Table(id, name) })
    }
  }

  def getTable(tableId: TableId): Future[Table] = for {
    (id, name) <- tableStruc.retrieve(tableId)
  } yield Table(id, name)

  def getRow(tableId: TableId, rowId: RowId): Future[Row] = {
    for {
      table <- getTable(tableId)
      allColumns <- getAllColumns(table)
      (rowId, values) <- rowStruc.get(tableId, rowId, allColumns)
      mergedValues <- {
        Future.sequence(allColumns map {
          case c: AttachmentColumn => attachmentModel.retrieveAll(c.table.id, c.id, rowId)
          case c: LinkColumn[_] => cellStruc.getLinkValues(c.table.id, c.id, rowId, c.to.table.id, c.to.id)
          case c: MultiLanguageColumn[_] => Future.successful(values.toList(c.id.toInt - 1))
          case c: SimpleValueColumn[_] => Future.successful(values.toList(c.id.toInt - 1))
        })
      }
    } yield Row(table, rowId, mergedValues)
  }

  def getRows(tableId: TableId): Future[RowSeq] = for {
    table <- getTable(tableId)
    rows <- getAllRows(table)
  } yield rows

  def getCell(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Cell[Any]] = for {
    column <- getColumn(tableId, columnId)
    value <- column match {
      case c: AttachmentColumn => attachmentModel.retrieveAll(c.table.id, c.id, rowId)
      case c: LinkColumn[_] => cellStruc.getLinkValues(c.table.id, c.id, rowId, c.to.table.id, c.to.id)
      case c: SimpleValueColumn[_] => cellStruc.getValue(c.table.id, c.id, rowId)
      case c: MultiLanguageColumn[_] => cellStruc.getTranslations(c.table.id, c.id, rowId)
    }
  } yield Cell(column.asInstanceOf[ColumnType[Any]], rowId, value)

  def getColumn(tableId: TableId, columnId: ColumnId): Future[ColumnType[_]] = for {
    table <- getTable(tableId)
    (columnId, columnName, columnKind, ordering, multilanguage) <- columnStruc.get(table.id, columnId)
    column <- columnKind match {
      case AttachmentType => getAttachmentColumn(table, columnId, columnName, ordering)
      case LinkType => getLinkColumn(table, columnId, columnName, ordering)

      case kind: TableauxDbType => getValueColumn(table, columnId, columnName, kind, ordering, multilanguage)
    }
  } yield column

  def getColumns(tableId: TableId): Future[ColumnSeq] = for {
    table <- getTable(tableId)
    columns <- getAllColumns(table)
  } yield ColumnSeq(columns)

  private def getValueColumn(table: Table, columnId: ColumnId, columnName: String, columnKind: TableauxDbType, ordering: Ordering, languageType: LanguageType): Future[ColumnType[_]] = {
    Future(Mapper(languageType, columnKind).apply(table, columnId, columnName, ordering))
  }

  private def getAttachmentColumn(table: Table, columnId: ColumnId, columnName: String, ordering: Ordering): Future[AttachmentColumn] = {
    Future(AttachmentColumn(table, columnId, columnName, ordering))
  }

  private def getLinkColumn(fromTable: Table, linkColumnId: ColumnId, columnName: String, ordering: Ordering): Future[LinkColumn[_]] = {
    for {
      (toTableId, toColumnId) <- columnStruc.getToColumn(fromTable.id, linkColumnId)
      toCol <- getColumn(toTableId, toColumnId).asInstanceOf[Future[SimpleValueColumn[_]]]
    } yield {
      LinkColumn(fromTable, linkColumnId, toCol, columnName, ordering)
    }
  }

  def getCompleteTable(tableId: TableId): Future[CompleteTable] = for {
    table <- getTable(tableId)
    colList <- getAllColumns(table)
    rowList <- getAllRows(table)
  } yield CompleteTable(table, colList, rowList)

  private def getAllColumns(table: Table): Future[Seq[ColumnType[_]]] = {
    for {
      columnSeq <- columnStruc.getAll(table.id)
      allColumns <- {
        val columns: Future[Seq[ColumnType[_]]] = Future.sequence({
          for {
            (columnId, columnName, kind, ordering, multilanguage) <- columnSeq
          } yield {
            kind match {
              case AttachmentType => getAttachmentColumn(table, columnId, columnName, ordering)
              case LinkType => getLinkColumn(table, columnId, columnName, ordering)

              case kind: TableauxDbType => getValueColumn(table, columnId, columnName, kind, ordering, multilanguage)
            }
          }
        })
        columns
      }
    } yield allColumns
  }

  private def getLinkColumns(table: Table): Future[Seq[LinkColumn[_]]] = {
    for {
      columnSeq <- columnStruc.getAll(table.id)
      filteredLinkColumns <- {
        Future.sequence({
          for {
            (columnId, columnName, columnKind, ordering, multilanguage) <- columnSeq if columnKind == LinkType
          } yield {
            getLinkColumn(table, columnId, columnName, ordering)
          }
        })
      }
    } yield filteredLinkColumns
  }

  private def getAllRows(table: Table): Future[RowSeq] = {
    for {
      allColumns <- getAllColumns(table)
      allRows <- rowStruc.getAll(table.id, allColumns)
      rowSeq <- {
        // TODO foreach row every link column is map and the link value is fetched!
        // TODO think about something with a better performance!
        val mergedRows = allRows map {
          case (rowId, values) =>
            val mergedValues = Future.sequence(allColumns map {
              case c: AttachmentColumn => attachmentModel.retrieveAll(c.table.id, c.id, rowId)
              case c: LinkColumn[_] => cellStruc.getLinkValues(c.table.id, c.id, rowId, c.to.table.id, c.to.id)

              case c: SimpleValueColumn[_] => Future.successful(values(c.id.toInt - 1))
              case c: MultiLanguageColumn[_] => Future.successful(values(c.id.toInt - 1))
            })

            (rowId, mergedValues)
        }

        val rows = mergedRows map {
          case (rowId, values) => values.map(Row(table, rowId, _))
        }

        Future.sequence(rows).map(seq => RowSeq(seq))
      }
    } yield rowSeq
  }

  def changeTableName(tableId: TableId, tableName: String): Future[Table] = for {
    _ <- tableStruc.changeName(tableId, tableName)
  } yield Table(tableId, tableName)

  def changeColumn(tableId: TableId, columnId: ColumnId, columnName: Option[String], ordering: Option[Ordering], kind: Option[TableauxDbType]): Future[ColumnType[_]] = for {
    _ <- columnStruc.change(tableId, columnId, columnName, ordering, kind)
    column <- getColumn(tableId, columnId)
  } yield column
}