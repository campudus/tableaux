package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.{InvalidJsonException, ArgumentChecker}
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.tableaux.{TableStructure, RowStructure, ColumnStructure, CellStructure}
import org.vertx.scala.core.json._

import scala.concurrent.Future
import scala.util.{Success, Try}

object TableauxModel {
  type IdType = Long

  type TableId = IdType
  type ColumnId = IdType
  type RowId = IdType

  type Ordering = Long

  /**
   * (ToTable, ToColumn, FromColumn)
   */
  type LinkConnection = (TableId, ColumnId, ColumnId)

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

  def deleteTable(id: IdType): Future[EmptyObject] = for {
    _ <- tableStruc.delete(id)
  } yield EmptyObject()

  def deleteRow(tableId: IdType, rowId: IdType): Future[EmptyObject] = for {
    _ <- rowStruc.delete(tableId, rowId)
  } yield EmptyObject()

  def addColumns(tableId: IdType, columns: Seq[CreateColumn]): Future[ColumnSeq] = for {
    cols <- serialiseFutures(columns) {
      case CreateSimpleColumn(name, kind, ordering) =>
        addValueColumn(tableId, name, kind, ordering)

      case CreateLinkColumn(name, ordering, (toTable, toColumn, fromColumn)) =>
        addLinkColumn(tableId, name, fromColumn, toTable, toColumn, ordering)

      case CreateAttachmentColumn(name, ordering) =>
        addAttachmentColumn(tableId, name, ordering)
    }
  } yield ColumnSeq(cols)

  def addValueColumn(tableId: IdType, name: String, columnType: TableauxDbType, ordering: Option[Ordering]): Future[SimpleValueColumn[_]] = for {
    table <- getTable(tableId)
    (id, ordering) <- columnStruc.insert(table.id, columnType, name, ordering)
  } yield Mapper(columnType).apply(table, id, name, ordering)

  def addLinkColumn(tableId: IdType, name: String, fromColumn: IdType, toTable: IdType, toColumn: IdType, ordering: Option[Ordering]): Future[LinkColumn[_]] = for {
    table <- getTable(tableId)
    toCol <- getColumn(toTable, toColumn).asInstanceOf[Future[SimpleValueColumn[_]]]
    (id, ordering) <- columnStruc.insertLink(tableId, name, fromColumn, toCol.table.id, toCol.id, ordering)
  } yield LinkColumn(table, id, toCol, name, ordering)

  def addAttachmentColumn(tableId: IdType, name: String, ordering: Option[Ordering]): Future[AttachmentColumn] = for {
    table <- getTable(tableId)
    (id, ordering) <- columnStruc.insertAttachment(table.id, name, ordering)
  } yield AttachmentColumn(table, id, name, ordering)

  def removeColumn(tableId: IdType, columnId: IdType): Future[EmptyObject] = for {
    _ <- columnStruc.delete(tableId, columnId)
  } yield EmptyObject()

  def addRow(tableId: IdType): Future[RowIdentifier] = for {
    table <- getTable(tableId)
    id <- rowStruc.create(tableId)
  } yield RowIdentifier(table, id)

  def addFullRows(tableId: IdType, values: Seq[Seq[(IdType, _)]]): Future[RowSeq] = for {
    table <- getTable(tableId)
    ids <- serialiseFutures(values)(rowStruc.createFull(table.id, _))
    row <- serialiseFutures(ids)(getRow(table.id, _))
  } yield RowSeq(row)

  private def serialiseFutures[A, B](seq: Seq[A])(fn: A => Future[B]): Future[Seq[B]] = {
    seq.foldLeft(Future(Seq.empty[B])) {
      (lastFuture, next) => lastFuture flatMap { preResults =>
        fn(next) map {
          preResults :+ _
        }
      }
    }
  }

  def updateValue[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_, _]] = for {
  // TODO column should be passed on to insert methods
    column <- getColumn(tableId, columnId)
    cell <- column match {
      case attachmentColumn: AttachmentColumn => updateAttachment(tableId, columnId, rowId, value)
      case _ => insertValue(tableId, columnId, rowId, value)
    }
  } yield cell

  def insertValue[A](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Cell[_, _]] = for {
    // TODO column should be passed on to insert methods
    column <- getColumn(tableId, columnId)
    cell <- column match {
      case attachmentColumn: AttachmentColumn => insertAttachment(tableId, columnId, rowId, value)
      case linkColumn: LinkColumn[_] => handleLinkValues(tableId, columnId, rowId, value)
      case _ => insertNormalValue(tableId, columnId, rowId, value)
    }
  } yield cell

  def insertAttachment[A](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Cell[AttachmentFile, AttachmentColumn]] = {
    handleAttachment(tableId, columnId, rowId, value, attachmentModel.add)
  }

  def updateAttachment[A](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Cell[AttachmentFile, AttachmentColumn]] = {
    handleAttachment(tableId, columnId, rowId, value, attachmentModel.update)
  }

  private def handleAttachment[A](tableId: IdType, columnId: IdType, rowId: IdType, value: A, fn: Attachment => Future[AttachmentFile]): Future[Cell[AttachmentFile, AttachmentColumn]] = {
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

  private def insertNormalValue[A, B <: ColumnType[A]](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Cell[A, B]] = for {
    column <- getColumn(tableId, columnId)
    _ <- cellStruc.update(tableId, columnId, rowId, value)
  } yield Cell(column.asInstanceOf[B], rowId, value)

  private def handleLinkValues[A](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Cell[Link[Any], LinkColumn[Any]]] = {
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

  private def insertLinkValues(tableId: IdType, columnId: IdType, rowId: IdType, from: IdType, tos: Seq[IdType]): Future[Cell[Link[Any], LinkColumn[Any]]] = for {
    linkColumn <- getColumn(tableId, columnId).asInstanceOf[Future[LinkColumn[Any]]]
    _ <- cellStruc.putLinks(linkColumn.table.id, linkColumn.id, from, tos)
    v <- cellStruc.getLinkValues(linkColumn.table.id, linkColumn.id, rowId, linkColumn.to.table.id, linkColumn.to.id)
  } yield Cell(linkColumn, rowId, Link(linkColumn.to.id, v))

  def addLinkValue(tableId: IdType, columnId: IdType, rowId: IdType, from: IdType, to: IdType): Future[Cell[Link[Any], LinkColumn[Any]]] = for {
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

  def getTable(tableId: IdType): Future[Table] = for {
    (id, name) <- tableStruc.retrieve(tableId)
  } yield Table(id, name)

  def getRow(tableId: IdType, rowId: IdType): Future[Row] = {
    for {
      table <- getTable(tableId)
      allColumns <- getAllColumns(table)
      (rowId, values) <- rowStruc.get(tableId, rowId, allColumns)
      mergedValues <- {
        Future.sequence(allColumns map {
          case c: AttachmentColumn => attachmentModel.retrieveAll(c.table.id, c.id, rowId)
          case c: LinkColumn[_] => cellStruc.getLinkValues(c.table.id, c.id, rowId, c.to.table.id, c.to.id)
          case c: SimpleValueColumn[_] => Future.successful(values.toList(c.id.toInt - 1))
        })
      }
    } yield Row(table, rowId, mergedValues)
  }

  def getRows(tableId: IdType): Future[RowSeq] = for {
    table <- getTable(tableId)
    rows <- getAllRows(table)
  } yield rows

  def getCell(tableId: IdType, columnId: IdType, rowId: IdType): Future[Cell[_, _]] = for {
    column <- getColumn(tableId, columnId)
    value <- column match {
      case c: AttachmentColumn => attachmentModel.retrieveAll(c.table.id, c.id, rowId)
      case c: LinkColumn[_] => cellStruc.getLinkValues(c.table.id, c.id, rowId, c.to.table.id, c.to.id)
      case c: SimpleValueColumn[_] => cellStruc.getValue(c.table.id, c.id, rowId)
    }
  } yield Cell(column.asInstanceOf[ColumnType[Any]], rowId, value)

  def getColumn(tableId: IdType, columnId: IdType): Future[ColumnType[_]] = for {
    table <- getTable(tableId)
    (columnId, columnName, columnKind, ordering) <- columnStruc.get(table.id, columnId)
    column <- columnKind match {
      case AttachmentType => getAttachmentColumn(table, columnId, columnName, ordering)
      case LinkType => getLinkColumn(table, columnId, columnName, ordering)

      case kind: TableauxDbType => getValueColumn(table, columnId, columnName, kind, ordering)
    }
  } yield column

  def getColumns(tableId: IdType): Future[ColumnSeq] = for {
    table <- getTable(tableId)
    columns <- getAllColumns(table)
  } yield ColumnSeq(columns)

  private def getValueColumn(table: Table, columnId: IdType, columnName: String, columnKind: TableauxDbType, ordering: Ordering): Future[SimpleValueColumn[_]] = {
    Future(Mapper(columnKind).apply(table, columnId, columnName, ordering))
  }

  private def getAttachmentColumn(table: Table, columnId: ColumnId, columnName: String, ordering: Ordering): Future[AttachmentColumn] = {
    Future(AttachmentColumn(table, columnId, columnName, ordering))
  }

  private def getLinkColumn(fromTable: Table, linkColumnId: IdType, columnName: String, ordering: Ordering): Future[LinkColumn[_]] = {
    for {
      (toTableId, toColumnId) <- columnStruc.getToColumn(fromTable.id, linkColumnId)
      toCol <- getColumn(toTableId, toColumnId).asInstanceOf[Future[SimpleValueColumn[_]]]
    } yield {
      LinkColumn(fromTable, linkColumnId, toCol, columnName, ordering)
    }
  }

  def getCompleteTable(tableId: IdType): Future[CompleteTable] = for {
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
            (columnId, columnName, kind, ordering) <- columnSeq
          } yield {
            kind match {
              case AttachmentType => getAttachmentColumn(table, columnId, columnName, ordering)
              case LinkType => getLinkColumn(table, columnId, columnName, ordering)

              case kind: TableauxDbType => getValueColumn(table, columnId, columnName, kind, ordering)
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
            (columnId, columnName, columnKind, ordering) <- columnSeq if columnKind == LinkType
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

  def changeTableName(tableId: IdType, tableName: String): Future[Table] = for {
    _ <- tableStruc.changeName(tableId, tableName)
  } yield Table(tableId, tableName)

  def changeColumn(tableId: IdType, columnId: IdType, columnName: Option[String], ordering: Option[Ordering], kind: Option[TableauxDbType]): Future[ColumnType[_]] = for {
    _ <- columnStruc.change(tableId, columnId, columnName, ordering, kind)
    column <- getColumn(tableId, columnId)
  } yield column
}