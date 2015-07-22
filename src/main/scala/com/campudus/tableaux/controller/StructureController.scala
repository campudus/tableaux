package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.helper.HelperFunctions._

import scala.concurrent.Future

object StructureController {
  def apply(config: TableauxConfig, repository: StructureModel): StructureController = {
    new StructureController(config, repository)
  }
}

class StructureController(override val config: TableauxConfig, override protected val repository: StructureModel) extends Controller[StructureModel] {

  lazy val tableStruc = repository.tableStruc
  lazy val columnStruc = repository.columnStruc

  def getTable(tableId: TableId): Future[Table] = {
    checkArguments(greaterZero(tableId))
    verticle.logger.info(s"getTable $tableId")

    tableStruc.retrieve(tableId)
  }

  def getAllTables(): Future[TableSeq] = {
    tableStruc.retrieveAll().map(TableSeq)
  }

  def createColumn(tableId: TableId, columns: Seq[CreateColumn]): Future[ColumnSeq] = for {
    cols <- serialiseFutures(columns) {
      case CreateSimpleColumn(name, ordering, kind, languageType) =>
        createValueColumn(tableId, name, kind, ordering, languageType)

      case CreateLinkColumn(name, ordering, linkConnection) =>
        createLinkColumn(tableId, name, linkConnection, ordering)

      case CreateAttachmentColumn(name, ordering) =>
        createAttachmentColumn(tableId, name, ordering)
    }
  } yield ColumnSeq(cols)

  private def createValueColumn(tableId: TableId, name: String, columnType: TableauxDbType, ordering: Option[Ordering], languageType: LanguageType): Future[ColumnType[_]] = for {
    table <- getTable(tableId)
    (id, ordering) <- columnStruc.insert(table.id, columnType, name, ordering, languageType)
  } yield Mapper(languageType, columnType).apply(table, id, name, ordering)

  private def createLinkColumn(tableId: TableId, name: String, linkConnection: LinkConnection, ordering: Option[Ordering]): Future[LinkColumn[_]] = for {
    table <- getTable(tableId)
    toCol <- getColumn(linkConnection.toTableId, linkConnection.toColumnId).asInstanceOf[Future[SimpleValueColumn[_]]]
    (id, ordering) <- columnStruc.insertLink(tableId, name, linkConnection, ordering)
  } yield LinkColumn(table, id, toCol, name, ordering)

  private def createAttachmentColumn(tableId: TableId, name: String, ordering: Option[Ordering]): Future[AttachmentColumn] = for {
    table <- getTable(tableId)
    (id, ordering) <- columnStruc.insertAttachment(table.id, name, ordering)
  } yield AttachmentColumn(table, id, name, ordering)


  def getCompleteTable(tableId: TableId): Future[CompleteTable] = {
    checkArguments(greaterZero(tableId))
    verticle.logger.info(s"getTable $tableId")

    for {
      table <- getTable(tableId)
      colList <- columnStruc.getAll(table.id)
      rowList <- repository.tableauxModel.getAllRows(table)
    } yield CompleteTable(table, colList, rowList)
  }

  def createTable(tableName: String, columns: Seq[CreateColumn], rowsValues: Seq[Seq[_]]): Future[CompleteTable] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $rowsValues")

    for {
      table <- createTable(tableName)
      columnIds <- createColumn(table.id, columns) map { colSeq => colSeq.columns map { col => col.id } }
      rowsWithColumnIdAndValue <- Future.successful {
        if (rowsValues.isEmpty) {
          Seq()
        } else {
          rowsValues map {
            columnIds.zip(_)
          }
        }
      }
      _ <- repository.tableauxModel.addFullRows(table.id, rowsWithColumnIdAndValue)
      completeTable <- getCompleteTable(table.id)
    } yield completeTable
  }

  def getColumn(tableId: TableId, columnId: ColumnId): Future[ColumnType[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"getColumn $tableId $columnId")

    for {
      column <- columnStruc.get(tableId, columnId)
    } yield column
  }

  def getColumns(tableId: TableId): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"getColumns $tableId")

    for {
      columns <- columnStruc.getAll(tableId)
    } yield ColumnSeq(columns)
  }

  def createTable(tableName: String): Future[Table] = {
    checkArguments(notNull(tableName, "TableName"))
    logger.info(s"createTable $tableName")

    for {
      id <- tableStruc.create(tableName)
    } yield Table(id, tableName)
  }

  def deleteTable(tableId: TableId): Future[EmptyObject] = for {
    _ <- tableStruc.delete(tableId)
  } yield EmptyObject()

  def deleteColumn(tableId: TableId, columnId: ColumnId): Future[EmptyObject] = for {
    _ <- columnStruc.delete(tableId, columnId)
  } yield EmptyObject()

  def changeTableName(tableId: TableId, tableName: String): Future[Table] = {
    checkArguments(greaterZero(tableId), notNull(tableName, "TableName"))
    logger.info(s"changeTableName $tableId $tableName")

    for {
      _ <- tableStruc.changeName(tableId, tableName)
    } yield Table(tableId, tableName)
  }

  def changeColumn(tableId: TableId,
                   columnId: ColumnId,
                   columnName: Option[String],
                   ordering: Option[Ordering],
                   kind: Option[TableauxDbType]): Future[ColumnType[_]] = {

    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"changeColumn $tableId $columnId $columnName $ordering $kind")

    for {
      _ <- columnStruc.change(tableId, columnId, columnName, ordering, kind)
      column <- getColumn(tableId, columnId)
    } yield column
  }
}
