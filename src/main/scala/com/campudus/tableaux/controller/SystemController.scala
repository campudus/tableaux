package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.cache.CacheClient
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.database.model.{StructureModel, SystemModel, TableauxModel}
import com.campudus.tableaux.helper.JsonUtils
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future
import scala.io.Source

object SystemController {
  val SETTING_LANGTAGS = "langtags"

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

    val development = "DEVELOPMENT"

    for {
      databaseVersion <- repository.retrieveCurrentVersion()
      specificationVersion = repository.retrieveSpecificationVersion()
    } yield {
      val json = Json.obj(
        "versions" -> Json.obj(
          "implementation" -> Json.obj(
            "vendor" -> manifestValue("Implementation-Vendor").getOrElse(development),
            "title" -> manifestValue("Implementation-Title").getOrElse(development),
            "version" -> manifestValue("Implementation-Version").getOrElse(development)
          ),
          "git" -> Json.obj(
            "branch" -> manifestValue("Git-Branch").getOrElse(development),
            "commit" -> manifestValue("Git-Commit").getOrElse(development),
            "date" -> manifestValue("Git-Committer-Date").getOrElse(development)
          ),
          "build" -> Json.obj(
            "date" -> manifestValue("Build-Date").getOrElse(development),
            "jdk" -> manifestValue("Build-Java-Version").getOrElse(development)
          ),
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
      linkColumn <- structureModel.columnStruc.createColumn(bl, CreateLinkColumn("Regierungsbezirke", None, rb.id, Some("Bundesland"), singleDirection = false, identifier = false, List()))

      toRow1 = generateToJson(1)
      toRow2 = generateToJson(2)

      // Bayern 2nd row
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 1, toRow2)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 2, toRow2)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 3, toRow2)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 4, toRow2)

      //Baden-Wuerttemberg 1st row
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 5, toRow1)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 6, toRow1)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 7, toRow1)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 8, toRow1)
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
      table <- structureModel.tableStruc.create(tableName, hidden = false, None, List())
      columns <- structureModel.columnStruc.createColumns(table, columns)

      columnIds = columns.map(_.id)
      rowsWithColumnIdAndValue = rows.map(columnIds.zip(_))

      _ <- tableauxModel.createRows(table, rowsWithColumnIdAndValue)
    } yield table
  }

  private def manifestValue(field: String): Option[String] = {
    import com.jcabi.manifests.Manifests

    if (Manifests.exists(field)) {
      Some(Manifests.read(field))
    } else {
      None
    }
  }

  def retrieveLangtags(): Future[DomainObject] = {
    repository.retrieveSetting(SystemController.SETTING_LANGTAGS)
      .map(value => PlainDomainObject(Json.obj("value" -> Option(value).map(f => Json.fromArrayString(f)).orNull)))
  }

  def invalidateCache(): Future[DomainObject] = {
    CacheClient(this.vertx).invalidateAll()
      .map(_ => EmptyObject())
  }

  def invalidateCache(tableId: TableId, columnId: ColumnId): Future[DomainObject] = {
    CacheClient(this.vertx).invalidateColumn(tableId, columnId)
      .map(_ => EmptyObject())
  }
}
