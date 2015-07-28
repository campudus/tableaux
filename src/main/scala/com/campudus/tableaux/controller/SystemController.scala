package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.{StructureModel, SystemModel, TableauxModel}
import com.campudus.tableaux.helper.FileUtils
import com.campudus.tableaux.helper.HelperFunctions._
import org.vertx.java.core.json.JsonObject

import scala.concurrent.Future

object SystemController {
  def apply(config: TableauxConfig, repository: SystemModel, tableauxModel: TableauxModel, structureModel: StructureModel): SystemController = {
    new SystemController(config, repository, tableauxModel, structureModel)
  }
}

class SystemController(override val config: TableauxConfig,
                       override protected val repository: SystemModel,
                       protected val tableauxModel: TableauxModel,
                       protected val structureModel: StructureModel) extends Controller[SystemModel] {

  def resetDB(): Future[DomainObject] = {
    logger.info("Reset system structure")

    repository.reset()
  }

  def createDemoTables(): Future[DomainObject] = {
    logger.info("Create demo tables")

    for {
      bl <- writeDemoData(readDemoData("bundeslaender"))
      rb <- writeDemoData(readDemoData("regierungsbezirke"))

      // Add link column Bundeslaender(Land) <> Regierungsbezirke(Regierungsbezirk)
      linkColumn <- structureModel.columnStruc.createColumn(bl, CreateLinkColumn("Bundesland", None, LinkConnection(1, rb.id, 1)))

      // Bayern 2nd row
      _ <- tableauxModel.addLinkValue(rb.id, linkColumn.id, 1, 2, 1)
      _ <- tableauxModel.addLinkValue(rb.id, linkColumn.id, 2, 2, 2)
      _ <- tableauxModel.addLinkValue(rb.id, linkColumn.id, 3, 2, 3)
      _ <- tableauxModel.addLinkValue(rb.id, linkColumn.id, 4, 2, 4)

      //Baden-Wuerttemberg 1st row
      _ <- tableauxModel.addLinkValue(rb.id, linkColumn.id, 5, 1, 5)
      _ <- tableauxModel.addLinkValue(rb.id, linkColumn.id, 6, 1, 6)
      _ <- tableauxModel.addLinkValue(rb.id, linkColumn.id, 6, 1, 7)
      _ <- tableauxModel.addLinkValue(rb.id, linkColumn.id, 8, 1, 8)
    } yield TableSeq(Seq(bl, rb))
  }

  private def writeDemoData(demoData: Future[JsonObject]): Future[Table] = {
    for {
      json <- demoData
      table <- createTable(json.getString("name"), jsonToSeqOfColumnNameAndType(json), jsonToSeqOfRowsWithValue(json))
    } yield table
  }

  private def readDemoData(name: String): Future[JsonObject] = {
    FileUtils(verticle).readJsonFile(s"demodata/$name.json")
  }

  private def createTable(tableName: String, columns: Seq[CreateColumn], rows: Seq[Seq[_]]): Future[Table] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $rows")

    for {
      table <- structureModel.tableStruc.create(tableName)
      columns <- structureModel.columnStruc.createColumns(table, columns)
      columnIds <- Future(columns.map(_.id))
      rowsWithColumnIdAndValue <- Future.successful(rows.map(columnIds.zip(_)))
      _ <- tableauxModel.createRows(table.id, rowsWithColumnIdAndValue)
    } yield table
  }
}
