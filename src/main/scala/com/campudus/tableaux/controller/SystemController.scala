package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.structure.{DomainObject, EmptyObject, CreateColumn}
import com.campudus.tableaux.database.model.{TableauxModel, SystemModel}
import com.campudus.tableaux.helper.HelperFunctions._
import org.vertx.scala.core.buffer.Buffer
import org.vertx.scala.core.json._

import scala.concurrent.{Promise, Future}
import scala.util.{Try, Failure, Success}

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
    for {
      _ <- writeDemoData(readDemoData())
    } yield EmptyObject()
  }

  private def writeDemoData(demoData: Future[Seq[JsonObject]]): Future[Seq[DomainObject]] = {
    demoData.flatMap { data =>
      val foo = data.map { table =>
        createTable(table.getString("tableName"), jsonToSeqOfColumnNameAndType(table), jsonToSeqOfRowsWithValue(table))
      }
      Future.sequence(foo)
    }
  }

  private def readDemoData(): Future[Seq[JsonObject]] = {
    readDir("../resources/demodata/", ".*\\.json") flatMap { fileNames =>
      Future.sequence((fileNames map { file =>
        readFile(file)
      }).toSeq)
    }
  }

  private def readDir(dir: String, filter: String): Future[Array[String]] = {
    import org.vertx.scala.core.FunctionConverters._

    val p = Promise[Array[String]]()

    vertx.fileSystem.readDir(dir, filter, {
      case Success(files) => p.success(files)
      case Failure(ex) =>
        logger.info("Failed reading schema directory")
        p.failure(ex)
    }: Try[Array[String]] => Unit)

    p.future
  }

  private def readFile(fileName: String): Future[JsonObject] = {
    import org.vertx.scala.core.FunctionConverters._

    val p = Promise[JsonObject]()

    vertx.fileSystem.readFile(fileName, {
      case Success(b) => p.success(Json.fromObjectString(b.toString()))
      case Failure(ex) =>
        logger.info("Failed reading schema file")
        p.failure(ex)
    }: Try[Buffer] => Unit)

    p.future
  }

  private def createTable(tableName: String, columns: => Seq[CreateColumn], rowsValues: Seq[Seq[_]]): Future[DomainObject] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $rowsValues")
    tableauxModel.createCompleteTable(tableName, columns, rowsValues)
  }
}
