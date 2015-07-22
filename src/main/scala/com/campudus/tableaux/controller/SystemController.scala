package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.model.{SystemModel, TableauxModel, StructureModel}
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.helper.FileUtils
import com.campudus.tableaux.helper.HelperFunctions._
import org.vertx.java.core.json.JsonObject
import org.vertx.scala.core.json._

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

  val fileProps = """^(.+[\\/])*(.+)\.(.+)$""".r

  val structureController = StructureController(config, structureModel)

  def resetDB(): Future[DomainObject] = {
    logger.info("Reset system structure")

    repository.reset()
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
      linkColumn <- structureModel.columnStruc.insertLink(
        tableId = getId(bl.getJson),
        name = "Bundesland",
        LinkConnection(fromColumnId = 1,
        toTableId = getId(rb.getJson),
        toColumnId = 1),
        ordering = None
      )

      // Bayern 2nd row
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn._1, 1, 2, 1)
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn._1, 2, 2, 2)
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn._1, 3, 2, 3)
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn._1, 4, 2, 4)

      //Baden-Wuerttemberg 1st row
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn._1, 5, 1, 5)
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn._1, 6, 1, 6)
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn._1, 6, 1, 7)
      _ <- tableauxModel.addLinkValue(getId(rb.getJson), linkColumn._1, 8, 1, 8)
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

  private def createTable(tableName: String, columns: Seq[CreateColumn], rowsValues: Seq[Seq[_]]): Future[CompleteTable] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $rowsValues")
    structureController.createTable(tableName, columns, rowsValues)
  }
}
