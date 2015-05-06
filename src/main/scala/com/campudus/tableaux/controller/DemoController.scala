package com.campudus.tableaux.controller

/**
 * Controller for resetting the database.
 * Mostly this is need for Demo & Test data.
 */

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.structure.CreateColumn
import com.campudus.tableaux.helper.HelperFunctions._
import com.campudus.tableaux.helper.StandardVerticle
import org.vertx.scala.core.buffer.Buffer
import org.vertx.scala.core.json._
import org.vertx.scala.platform.Verticle

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class DemoController(val verticle: Verticle, val database: DatabaseConnection) extends DatabaseAccess with StandardVerticle {

  lazy val tableaux = new Tableaux(verticle, database)

  val fileProps = """^(.+[\\/])*(.+)\.(.+)$""".r

  def resetDB(): Future[DomainObject] = {
    logger.info("Reset database")
    tableaux.resetDB()
  }

  def createDemoTables(): Future[DomainObject] = {
    for {
      _ <- writeDemoData(readDemoData())
      emptyObject <- Future.successful(EmptyObject())
    } yield emptyObject
  }

  private def writeDemoData(demoData: Future[Seq[JsonObject]]): Future[DomainObject] = {
    val p = Promise[DomainObject]()

    //TODO It works but I'm not sure if it's nice
    demoData.map { data =>
      data.map { table =>
        createTable(table.getString("tableName"), jsonToSeqOfColumnNameAndType(table), jsonToSeqOfRowsWithValue(table))
        p.success(EmptyObject())
      }
    }

    p.future
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
    tableaux.createCompleteTable(tableName, columns, rowsValues)
  }
}
