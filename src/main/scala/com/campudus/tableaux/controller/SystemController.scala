package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.{StructureModel, SystemModel, TableauxModel}
import com.campudus.tableaux.helper.JsonUtils
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future
import scala.io.Source

object SystemController {
  def apply(config: TableauxConfig, repository: SystemModel, tableauxModel: TableauxModel, structureModel: StructureModel): SystemController = {
    new SystemController(config, repository, tableauxModel, structureModel)
  }
}

class SystemController(override val config: TableauxConfig,
                       override protected val repository: SystemModel,
                       protected val tableauxModel: TableauxModel,
                       protected val structureModel: StructureModel) extends Controller[SystemModel] {

  def retrieveVersions(): Future[DomainObject] = {
    logger.info("Retrieve system version")

    val objPackage = getClass.getPackage

    for {
      databaseVersion <- repository.retrieveCurrentVersion()
      specificationVersion = repository.retrieveSpecificationVersion()
    } yield {
      val json = Json.obj(
        "versions" -> Json.obj(
          "implementation" -> Option(objPackage.getImplementationVersion).getOrElse("DEVELOPMENT"),
          "database" -> Json.obj(
            "current" -> databaseVersion,
            "specification" -> specificationVersion
          )
        )
      )

      PlainDomainObject(json)
    }
  }

  def updateDB(): Future[DomainObject] = {
    logger.info("Update system structure")

    for {
      _ <- repository.update()
      version <- retrieveVersions()
    } yield PlainDomainObject(Json.obj("updated" -> true).mergeIn(version.getJson))
  }

  def resetDB(): Future[DomainObject] = {
    logger.info("Reset system structure")

    for {
      _ <- repository.uninstall()
      _ <- repository.install()
    } yield EmptyObject()
  }

  def createDemoTables(): Future[DomainObject] = {
    logger.info("Create demo tables")

    def generateToJson(to: Int): JsonObject = {
      Json.obj("to" -> to)
    }

    for {
      bl <- writeDemoData(readDemoData("bundeslaender"))
      rb <- writeDemoData(readDemoData("regierungsbezirke"))

      // Add link column Bundeslaender(Land) <> Regierungsbezirke(Regierungsbezirk)
      linkColumn <- structureModel.columnStruc.createColumn(bl, CreateLinkColumn("Regierungsbezirke", None, LinkConnection(rb.id, 1, 1), Some("Bundesland"), singleDirection = false, identifier = false))

      toRow1 = generateToJson(1)
      toRow2 = generateToJson(2)

      // Bayern 2nd row
      _ <- tableauxModel.insertValue(rb.id, linkColumn.id, 1, toRow2)
      _ <- tableauxModel.insertValue(rb.id, linkColumn.id, 2, toRow2)
      _ <- tableauxModel.insertValue(rb.id, linkColumn.id, 3, toRow2)
      _ <- tableauxModel.insertValue(rb.id, linkColumn.id, 4, toRow2)

      //Baden-Wuerttemberg 1st row
      _ <- tableauxModel.insertValue(rb.id, linkColumn.id, 5, toRow1)
      _ <- tableauxModel.insertValue(rb.id, linkColumn.id, 6, toRow1)
      _ <- tableauxModel.insertValue(rb.id, linkColumn.id, 7, toRow1)
      _ <- tableauxModel.insertValue(rb.id, linkColumn.id, 8, toRow1)
    } yield TableSeq(Seq(bl, rb))
  }

  private def writeDemoData(json: JsonObject): Future[Table] = {
    createTable(json.getString("name"), JsonUtils.toCreateColumnSeq(json), JsonUtils.toRowValueSeq(json))
  }

  private def readDemoData(name: String): JsonObject = {
    val file = Source.fromInputStream(getClass.getResourceAsStream(s"/demodata/$name.json"), "UTF-8").mkString
    Json.fromObjectString(file)
  }

  private def createTable(tableName: String, columns: Seq[CreateColumn], rows: Seq[Seq[_]]): Future[Table] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $rows")

    for {
      tables <- structureModel.tableStruc.retrieveAll()
      c <- structureModel.columnStruc.retrieveAll(1)
    } yield {
      logger.info(s"after retrieve $tables $c")
    }

    for {
      table <- structureModel.tableStruc.create(tableName)
      columns <- structureModel.columnStruc.createColumns(table, columns)

      columnIds = columns.map(_.id)
      rowsWithColumnIdAndValue = rows.map(columnIds.zip(_))

      _ <- tableauxModel.createRows(table.id, rowsWithColumnIdAndValue)
    } yield table
  }
}
