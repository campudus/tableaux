package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.tableaux.{CellModel, RowModel}
import com.campudus.tableaux.helper.HelperFunctions._
import com.campudus.tableaux.{ArgumentChecker, InvalidJsonException}
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

  val cellStruc = new CellModel(connection)
  val rowStruc = new RowModel(connection)

  lazy val attachmentModel = AttachmentModel(connection)
  private lazy val structureModel = StructureModel(connection)

  def createTable(name: String): Future[Table] = {
    structureModel.tableStruc.create(name)
  }

  def createColumns(tableId: TableId, columns: Seq[CreateColumn]): Future[Seq[ColumnType[_]]] = {
    for {
      table <- getTable(tableId)
      columns <- structureModel.columnStruc.createColumns(table, columns)
    } yield columns
  }

  def getTable(tableId: TableId): Future[Table] = {
    structureModel.tableStruc.retrieve(tableId)
  }

  def getColumn(tableId: TableId, columnId: ColumnId): Future[ColumnType[_]] = {
    structureModel.columnStruc.retrieve(tableId, columnId)
  }

  def getAllColumns(tableId: TableId): Future[Seq[ColumnType[_]]] = {
    structureModel.columnStruc.retrieveAll(tableId)
  }

  def deleteRow(tableId: TableId, rowId: RowId): Future[EmptyObject] = for {
    _ <- rowStruc.delete(tableId, rowId)
  } yield EmptyObject()

  def addRow(tableId: TableId): Future[Row] = for {
    table <- getTable(tableId)
    id <- rowStruc.createEmpty(tableId)
  } yield Row(table, id, Seq.empty)

  def addFullRows(tableId: TableId, rows: Seq[Seq[(ColumnId, Any)]]): Future[RowSeq] = for {
    table <- getTable(tableId)
    columns <- getAllColumns(table.id)
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
            Future(())
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

  def getRow(tableId: TableId, rowId: RowId): Future[Row] = {
    for {
      table <- getTable(tableId)
      allColumns <- getAllColumns(table.id)
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

  def getAllRows(table: Table): Future[RowSeq] = {
    for {
      allColumns <- getAllColumns(table.id)
      allRows <- rowStruc.getAll(table.id, allColumns)
      rowSeq <- {
        // TODO performance problem: foreach row every link column is map and the link value is fetched!
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
}