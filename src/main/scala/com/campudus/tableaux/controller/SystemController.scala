package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.model.{SystemModel, TableauxModel}
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.helper.FileUtils
import com.campudus.tableaux.helper.HelperFunctions._
import org.vertx.java.core.json.JsonObject
import org.vertx.scala.core.json._

import scala.concurrent.Future

object SystemController {
  def apply(config: TableauxConfig, repository: SystemModel, tableauxModel: TableauxModel): SystemController = {
    new SystemController(config, repository, tableauxModel)
  }
}

class SystemController(override val config: TableauxConfig,
                       override protected val repository: SystemModel,
                       protected val tableauxModel: TableauxModel) extends Controller[SystemModel] {

  val fileProps = """^(.+[\\/])*(.+)\.(.+)$""".r

  def resetDB(): Future[DomainObject] = {
    logger.info("Reset database")
    for {
      _ <- repository.deinstall()
      _ <- repository.setup()
    } yield EmptyObject()
  }

  def createDemoTables(): Future[DomainObject] = {
    logger.info("Create demo tables")

    val getId = { o: JsonObject =>
      o.getLong("id")
    }

    for {
      bl <- writeDemoData(readDemoData("bundeslaender"))
      rb <- writeDemoData(readDemoData("regierungsbezirke"))

      // Add link column Bundeslaender(Land) <> Regierungsbezirke(Regierungsbezirk)
      linkColumn <- tableauxModel.addLinkColumn(
        tableId = getId(bl.getJson),
        name = "Bundesland",
        LinkConnection(fromColumnId = 1,
        toTableId = getId(rb.getJson),
        toColumnId = 1),
        ordering = None
      )

      // Bayern 2nd row
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn.id, 1, 2, 1)
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn.id, 2, 2, 2)
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn.id, 3, 2, 3)
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn.id, 4, 2, 4)

      //Baden-Wuerttemberg 1st row
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn.id, 5, 1, 5)
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn.id, 6, 1, 6)
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn.id, 6, 1, 7)
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn.id, 8, 1, 8)
    } yield EmptyObject()
  }

  private def writeDemoData(demoData: Future[JsonObject]): Future[CompleteTable] = {
    for {
      table <- demoData
      completeTable <- createTable(table.getString("name"), jsonToSeqOfColumnNameAndType(table), jsonToSeqOfRowsWithValue(table))
    } yield completeTable
  }

  private def readDemoData(name: String): Future[JsonObject] = {
    FileUtils(verticle).readJsonFile(s"demodata/$name.json")
  }

  private def createTable(tableName: String, columns: => Seq[CreateColumn], rowsValues: Seq[Seq[_]]): Future[CompleteTable] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $rowsValues")
    tableauxModel.createCompleteTable(tableName, columns, rowsValues)
  }
}
