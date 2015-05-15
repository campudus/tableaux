package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.model.{SystemModel, TableauxModel}
import com.campudus.tableaux.database.structure.{CreateColumn, DomainObject, EmptyObject}
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
      _ <- writeDemoData(readDemoData())
    } yield EmptyObject()
  }

  private def writeDemoData(demoData: Future[Seq[JsonObject]]): Future[Seq[DomainObject]] = {
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

  private def createTable(tableName: String, columns: => Seq[CreateColumn], rowsValues: Seq[Seq[_]]): Future[DomainObject] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $rowsValues")
    tableauxModel.createCompleteTable(tableName, columns, rowsValues)
  }
}
