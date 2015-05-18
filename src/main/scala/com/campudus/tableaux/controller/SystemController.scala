package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.model.{SystemModel, TableauxModel}
import com.campudus.tableaux.database.structure.{CompleteTable, CreateColumn, DomainObject, EmptyObject}
import com.campudus.tableaux.helper.FileUtils
import com.campudus.tableaux.helper.HelperFunctions._
import org.vertx.scala.core.json._

import scala.concurrent.Future

object SystemController {
  def apply(config: TableauxConfig, repository: SystemModel, tableauxModel: TableauxModel): SystemController = {
    new SystemController(config, repository, tableauxModel)
  }
}

class SystemController(override val config: TableauxConfig, override protected val repository: SystemModel, protected val tableauxModel: TableauxModel) extends Controller[SystemModel] {
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
    for {
      tableIds <- writeDemoData(readDemoData())
      linkId <- tableauxModel.addLinkColumn(1, "Bundesland", 1, 2, 1, Some(5))
      //Bayern
      _ <- tableauxModel.insertLinkValue(2, 5, 1, (2,1))
      _ <- tableauxModel.insertLinkValue(2, 5, 2, (2,2))
      _ <- tableauxModel.insertLinkValue(2, 5, 3, (2,3))
      _ <- tableauxModel.insertLinkValue(2, 5, 4, (2,4))
      //Baden-Wuerttemberg
      _ <- tableauxModel.insertLinkValue(2, 5, 1, (1,5))
      _ <- tableauxModel.insertLinkValue(2, 5, 2, (1,6))
      _ <- tableauxModel.insertLinkValue(2, 5, 3, (1,7))
      _ <- tableauxModel.insertLinkValue(2, 5, 4, (1,8))
    } yield EmptyObject()
  }

  private def writeDemoData(demoData: Future[Seq[JsonObject]]): Future[Seq[CompleteTable]] = {
    demoData.flatMap { data =>
      val foo = data.map { table =>
        createTable(table.getString("name"), jsonToSeqOfColumnNameAndType(table), jsonToSeqOfRowsWithValue(table))
      }
      Future.sequence(foo)
    }
  }

  private def readDemoData(): Future[Seq[JsonObject]] = {
    FileUtils(verticle).readDir("../resources/demodata/", ".*\\.json") flatMap { fileNames =>
      Future.sequence(
        (fileNames map { file =>
          FileUtils(verticle).readJsonFile(file)
        }).toSeq
      )
    }
  }

  private def createTable(tableName: String, columns: => Seq[CreateColumn], rowsValues: Seq[Seq[_]]): Future[CompleteTable] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $rowsValues")
    tableauxModel.createCompleteTable(tableName, columns, rowsValues)
  }
}
