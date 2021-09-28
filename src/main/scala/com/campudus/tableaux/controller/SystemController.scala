package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.cache.CacheClient
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.ServiceModel.ServiceId
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.database.model.{ServiceModel, StructureModel, SystemModel, TableauxModel}
import com.campudus.tableaux.helper.JsonUtils
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.{RequestContext, TableauxConfig}
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future
import scala.io.Source

object SystemController {
  val SETTING_LANGTAGS = "langtags"
  val SETTING_SENTRY_URL = "sentryUrl"

  def apply(
      config: TableauxConfig,
      repository: SystemModel,
      tableauxModel: TableauxModel,
      structureModel: StructureModel,
      serviceModel: ServiceModel,
      roleModel: RoleModel
  )(implicit requestContext: RequestContext): SystemController = {
    new SystemController(config, repository, tableauxModel, structureModel, serviceModel, roleModel)
  }
}

case class SchemaVersion(databaseVersion: Int, specificationVersion: Int)

class SystemController(
    override val config: TableauxConfig,
    override protected val repository: SystemModel,
    protected val tableauxModel: TableauxModel,
    protected val structureModel: StructureModel,
    protected val serviceModel: ServiceModel,
    implicit protected val roleModel: RoleModel
)(implicit requestContext: RequestContext)
    extends Controller[SystemModel] {

  def retrieveSchemaVersion(): Future[SchemaVersion] = {
    for {
      databaseVersion <- repository.retrieveCurrentVersion()
      specificationVersion = repository.retrieveSpecificationVersion()
    } yield SchemaVersion(databaseVersion, specificationVersion)
  }

  def retrieveVersions(): Future[DomainObject] = {
    logger.info("Retrieve version information")

    val development = "DEVELOPMENT"

    for {
      schemaVersion <- retrieveSchemaVersion()
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
            "current" -> schemaVersion.databaseVersion,
            "specification" -> schemaVersion.specificationVersion
          )
        )
      )

      PlainDomainObject(json)
    }
  }

  def updateDB(): Future[DomainObject] = {
    logger.info("Update system structure")

    for {
      _ <- roleModel.checkAuthorization(Edit, ScopeSystem)
      _ <- repository.update()
      version <- retrieveVersions()
    } yield PlainDomainObject(Json.obj("updated" -> true).mergeIn(version.getJson))
  }

  def resetDB(): Future[DomainObject] = {
    logger.info("Reset system structure")

    for {
      _ <- roleModel.checkAuthorization(Edit, ScopeSystem)
      _ <- repository.uninstall()
      _ <- repository.install()
      _ <- invalidateCache()
      _ <- structureModel.columnStruc.removeAllCache()
    } yield EmptyObject()
  }

  def createDemoTables(): Future[DomainObject] = {
    logger.info("Create demo tables")

    def generateToJson(to: Int): JsonObject = {
      Json.obj("to" -> to)
    }

    for {
      _ <- roleModel.checkAuthorization(Edit, ScopeSystem)
      bl <- writeDemoData(readDemoData("bundeslaender"))
      rb <- writeDemoData(readDemoData("regierungsbezirke"))

      // Add link column Bundeslaender(Land) <> Regierungsbezirke(Regierungsbezirk)
      linkColumn <- structureModel.columnStruc.createColumn(
        bl,
        CreateLinkColumn(
          "Regierungsbezirke",
          None,
          rb.id,
          Some("Bundesland"),
          None,
          singleDirection = false,
          identifier = false,
          List(),
          Constraint(Cardinality(1, 0), deleteCascade = false),
          Option(new JsonObject("{}"))
        )
      )

      linkToBadenWuerttemberg = generateToJson(1)
      linkToBayern = generateToJson(2)
      linkToHessen = generateToJson(7)

      //Baden-Wuerttemberg 1st row
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 8, linkToBadenWuerttemberg)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 9, linkToBadenWuerttemberg)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 10, linkToBadenWuerttemberg)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 11, linkToBadenWuerttemberg)

      // Bayern 2nd row
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 1, linkToBayern)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 2, linkToBayern)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 3, linkToBayern)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 4, linkToBayern)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 5, linkToBayern)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 6, linkToBayern)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 7, linkToBayern)

      //Hessen 7st row
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 12, linkToHessen)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 13, linkToHessen)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 14, linkToHessen)

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
      table <- structureModel.tableStruc.create(tableName, hidden = false, None, List(), GenericTable, None, None)
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
    repository
      .retrieveSetting(SystemController.SETTING_LANGTAGS)
      .map(valueOpt => PlainDomainObject(Json.obj("value" -> valueOpt.map(f => Json.fromArrayString(f)).orNull)))
  }

  def retrieveSentryUrl(): Future[DomainObject] = {
    repository
      .retrieveSetting(SystemController.SETTING_SENTRY_URL)
      .map(valueOpt => PlainDomainObject(Json.obj("value" -> valueOpt.orNull)))
  }

  def updateLangtags(langtags: Seq[String]): Future[DomainObject] = {
    for {
      _ <- roleModel.checkAuthorization(Edit, ScopeSystem)
      _ <- repository.updateSetting(SystemController.SETTING_LANGTAGS, Json.arr(langtags: _*).toString)
      updatedLangtags <- retrieveLangtags()
    } yield updatedLangtags
  }

  def updateSentryUrl(sentryUrl: String): Future[DomainObject] = {
    for {
      _ <- roleModel.checkAuthorization(Edit, ScopeSystem)
      _ <- repository.updateSetting(SystemController.SETTING_SENTRY_URL, sentryUrl)
      updatedSentryUrl <- retrieveSentryUrl()
    } yield updatedSentryUrl
  }

  def invalidateCache(): Future[DomainObject] = {
    CacheClient(this)
      .invalidateAll()
      .map(_ => EmptyObject())
  }

  def invalidateCache(tableId: TableId, columnId: ColumnId): Future[DomainObject] = {
    CacheClient(this)
      .invalidateColumn(tableId, columnId)
      .map(_ => EmptyObject())
  }

  def createService(
      name: String,
      serviceType: ServiceType,
      ordering: Option[Long],
      displayName: MultiLanguageValue[String],
      description: MultiLanguageValue[String],
      active: Boolean,
      config: Option[JsonObject],
      scope: Option[JsonObject]
  ): Future[DomainObject] = {

    checkArguments(
      notNull(name, "name"),
      notNull(serviceType, "type")
    )

    logger.info(s"createService $name $serviceType $ordering $displayName $description $active $config $scope")

    for {
      _ <- roleModel.checkAuthorization(Create, ScopeService)
      serviceId <- serviceModel.create(name, serviceType, ordering, displayName, description, active, config, scope)
      service <- retrieveService(serviceId)
    } yield service
  }

  def updateService(
      serviceId: ServiceId,
      name: Option[String],
      serviceType: Option[ServiceType],
      ordering: Option[Long],
      displayName: Option[MultiLanguageValue[String]],
      description: Option[MultiLanguageValue[String]],
      active: Option[Boolean],
      config: Option[JsonObject],
      scope: Option[JsonObject]
  ): Future[DomainObject] = {

    checkArguments(
      greaterZero(serviceId),
      isDefined(
        Seq(name, serviceType, ordering, displayName, description, active, config, scope),
        "name, type, ordering, displayName, description, active, config, scope"
      )
    )

    val structureProperties: Seq[Option[Any]] = Seq(name, serviceType, config, scope)
    val isAtLeastOneStructureProperty: Boolean = structureProperties.exists(_.isDefined)

    logger.info(
      s"updateService $serviceId $name $serviceType $ordering $displayName $description $active $config $scope")

    for {
      _ <- if (isAtLeastOneStructureProperty) {
        roleModel.checkAuthorization(EditStructureProperty, ScopeService)
      } else {
        roleModel.checkAuthorization(EditDisplayProperty, ScopeService)
      }
      _ <- serviceModel.update(serviceId, name, serviceType, ordering, displayName, description, active, config, scope)
      service <- retrieveService(serviceId)
    } yield service
  }

  def retrieveServices(): Future[ServiceSeq] = {
    logger.info(s"retrieveServices")
    for {
      serviceSeq <- serviceModel.retrieveAll()
    } yield {
      val filteredServices: Seq[Service] =
        roleModel.filterDomainObjects[Service](ScopeService, serviceSeq, isInternalCall = false)
      ServiceSeq(filteredServices)
    }
  }

  def retrieveService(serviceId: ServiceId): Future[Service] = {
    logger.info(s"retrieveService $serviceId")
    for {
      _ <- roleModel.checkAuthorization(View, ScopeService)
      service <- serviceModel.retrieve(serviceId)
    } yield service
  }

  def deleteService(serviceId: ServiceId): Future[DomainObject] = {
    logger.info(s"deleteService $serviceId")

    for {
      _ <- roleModel.checkAuthorization(Delete, ScopeService)
      _ <- serviceModel.delete(serviceId)
    } yield EmptyObject()
  }
}
