package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.{
  CellAnnotationConfigModel,
  ServiceModel,
  StructureModel,
  SystemModel,
  TableauxModel
}
import com.campudus.tableaux.database.model.ServiceModel.ServiceId
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.helper.JsonUtils
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.verticles.EventClient

import io.vertx.scala.ext.web.RoutingContext
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
      roleModel: RoleModel,
      cellAnnotationConfigModel: CellAnnotationConfigModel
  ): SystemController = {
    new SystemController(
      config,
      repository,
      tableauxModel,
      structureModel,
      serviceModel,
      roleModel,
      cellAnnotationConfigModel
    )
  }
}

case class SchemaVersion(databaseVersion: Int, specificationVersion: Int)

class SystemController(
    override val config: TableauxConfig,
    override protected val repository: SystemModel,
    protected val tableauxModel: TableauxModel,
    protected val structureModel: StructureModel,
    protected val serviceModel: ServiceModel,
    implicit protected val roleModel: RoleModel,
    protected val cellAnnotationConfigModel: CellAnnotationConfigModel
) extends Controller[SystemModel] {
  val eventClient: EventClient = EventClient(vertx)

  def retrieveSchemaVersion(): Future[SchemaVersion] = {
    for {
      databaseVersion <- repository.retrieveCurrentVersion()
      specificationVersion = repository.retrieveSpecificationVersion()
    } yield SchemaVersion(databaseVersion, specificationVersion)
  }

  def retrieveVersions(): Future[DomainObject] = {
    logger.debug("Retrieve version information")

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

  def updateDB()(implicit user: TableauxUser): Future[DomainObject] = {
    logger.info("Update system structure")

    for {
      _ <- roleModel.checkAuthorization(EditSystem)
      _ <- repository.update()
      version <- retrieveVersions()
    } yield PlainDomainObject(Json.obj("updated" -> true).mergeIn(version.getJson))
  }

  def resetDB()(implicit user: TableauxUser): Future[DomainObject] = {
    logger.info("Reset system structure")

    for {
      _ <- roleModel.checkAuthorization(EditSystem)
      _ <- repository.uninstall()
      _ <- repository.installShortCutFunction()
      _ <- repository.install()
      _ <- invalidateCache()
      _ <- structureModel.columnStruc.removeAllCache()
    } yield EmptyObject()
  }

  def createDemoTables()(implicit user: TableauxUser): Future[DomainObject] = {
    logger.info("Create demo tables")

    def generateToJson(to: Int): JsonObject = {
      Json.obj("to" -> to)
    }

    for {
      _ <- roleModel.checkAuthorization(EditSystem)
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
          Option(Json.obj())
        )
      )

      linkToBadenWuerttemberg = generateToJson(1)
      linkToBayern = generateToJson(2)
      linkToHessen = generateToJson(7)

      // Baden-Wuerttemberg 1st row
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

      // Hessen 7st row
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 12, linkToHessen)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 13, linkToHessen)
      _ <- tableauxModel.replaceCellValue(rb, linkColumn.id, 14, linkToHessen)

    } yield TableSeq(Seq(bl, rb))
  }

  private def writeDemoData(json: JsonObject)(implicit user: TableauxUser): Future[Table] = {
    createTable(json.getString("name"), JsonUtils.toCreateColumnSeq(json), JsonUtils.toRowValueSeq(json))
  }

  private def readDemoData(name: String): JsonObject = {
    val file = Source.fromInputStream(getClass.getResourceAsStream(s"/demodata/$name.json"), "UTF-8").mkString
    Json.fromObjectString(file)
  }

  private def createTable(tableName: String, columns: Seq[CreateColumn], rows: Seq[Seq[_]])(
      implicit user: TableauxUser
  ): Future[Table] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $rows")

    for {
      table <- structureModel.tableStruc.create(tableName, hidden = false, None, List(), GenericTable, None, None, None)
      columns <- structureModel.columnStruc.createColumns(table, columns)

      columnIds = columns.map(_.id)
      rowsWithColumnIdAndValue = rows.map(columnIds.zip(_))

      // don't pass row permissions here, because method is only used for demo data
      _ <- tableauxModel.createRows(table, rowsWithColumnIdAndValue, None)
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

  def updateLangtags(langtags: Seq[String])(implicit user: TableauxUser): Future[DomainObject] = {
    for {
      _ <- roleModel.checkAuthorization(EditSystem)
      _ <- repository.updateSetting(SystemController.SETTING_LANGTAGS, Json.arr(langtags: _*).toString)
      updatedLangtags <- retrieveLangtags()
    } yield updatedLangtags
  }

  def updateSentryUrl(sentryUrl: String)(implicit user: TableauxUser): Future[DomainObject] = {
    for {
      _ <- roleModel.checkAuthorization(EditSystem)
      _ <- repository.updateSetting(SystemController.SETTING_SENTRY_URL, sentryUrl)
      updatedSentryUrl <- retrieveSentryUrl()
    } yield updatedSentryUrl
  }

  def invalidateCache(): Future[DomainObject] = {
    eventClient
      .invalidateAll()
      .map(_ => EmptyObject())
  }

  def invalidateCache(tableId: TableId, columnId: ColumnId): Future[DomainObject] = {
    eventClient
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
  )(implicit user: TableauxUser): Future[DomainObject] = {

    checkArguments(
      notNull(name, "name"),
      notNull(serviceType, "type")
    )

    logger.info(s"createService $name $serviceType $ordering $displayName $description $active $config $scope")

    for {
      _ <- roleModel.checkAuthorization(CreateService)
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
  )(implicit user: TableauxUser): Future[DomainObject] = {

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
      s"updateService $serviceId $name $serviceType $ordering $displayName $description $active $config $scope"
    )

    for {
      _ <-
        if (isAtLeastOneStructureProperty) {
          roleModel.checkAuthorization(EditServiceStructureProperty)
        } else {
          roleModel.checkAuthorization(EditServiceDisplayProperty)
        }
      _ <- serviceModel.update(serviceId, name, serviceType, ordering, displayName, description, active, config, scope)
      service <- retrieveService(serviceId)
    } yield service
  }

  def retrieveServices()(implicit user: TableauxUser): Future[ServiceSeq] = {
    logger.info(s"retrieveServices")
    for {
      serviceSeq <- serviceModel.retrieveAll()
    } yield {
      val filteredServices: Seq[Service] =
        roleModel.filterDomainObjects[Service](ViewService, serviceSeq, isInternalCall = false)
      ServiceSeq(filteredServices)
    }
  }

  def retrieveService(serviceId: ServiceId)(implicit user: TableauxUser): Future[Service] = {
    logger.info(s"retrieveService $serviceId")
    for {
      _ <- roleModel.checkAuthorization(ViewService)
      service <- serviceModel.retrieve(serviceId)
    } yield service
  }

  def deleteService(serviceId: ServiceId)(implicit user: TableauxUser): Future[DomainObject] = {
    logger.info(s"deleteService $serviceId")

    for {
      _ <- roleModel.checkAuthorization(DeleteService)
      _ <- serviceModel.delete(serviceId)
    } yield EmptyObject()
  }

  def retrieveCellAnnotationConfigs()(implicit user: TableauxUser): Future[DomainObject] = {
    logger.info(s"retrieveCellAnnotationConfigs")
    for {
      annotations <- cellAnnotationConfigModel.retrieveAll()
    } yield {
      val json = Json.obj("annotations" -> annotations.map(_.getJson))

      PlainDomainObject(json)
    }
  }

  def retrieveCellAnnotationConfig(annotationName: String)(implicit
  user: TableauxUser): Future[CellAnnotationConfig] = {
    logger.info(s"retrieveCellAnnotationConfig $annotationName")

    for {
      _ <- roleModel.checkAuthorization(ViewCellAnnotationConfig)
      annotation <- cellAnnotationConfigModel.retrieve(annotationName)
    } yield annotation
  }

  def deleteCellAnnotationConfig(annotationName: String)(implicit user: TableauxUser): Future[DomainObject] = {
    logger.info(s"deleteCellAnnotationConfig $annotationName")

    for {
      _ <- roleModel.checkAuthorization(DeleteCellAnnotationConfig)
      _ <- cellAnnotationConfigModel.delete(annotationName)
    } yield EmptyObject()
  }

  def updateCellAnnotationConfig(
      name: String,
      priority: Option[Int],
      fgColor: Option[String],
      bgColor: Option[String],
      displayName: Option[MultiLanguageValue[String]],
      isMultilang: Option[Boolean],
      isDashboard: Option[Boolean]
  )(implicit user: TableauxUser): Future[DomainObject] = {

    checkArguments(
      notNull(name, "name"),
      isDefined(
        Seq(priority, fgColor, bgColor, displayName, isMultilang, isDashboard),
        "priority, fgColor, bgColor, displayName, isMultilang, isDashboard"
      )
    )

    logger.info(
      s"updateCellAnnotationConfig $name $priority $fgColor $bgColor $displayName $isMultilang $isDashboard"
    )

    for {
      _ <- roleModel.checkAuthorization(EditCellAnnotationConfig)
      _ <- cellAnnotationConfigModel.update(name, priority, fgColor, bgColor, displayName, isMultilang, isDashboard)
      annotationConfig <- retrieveCellAnnotationConfig(name)
    } yield annotationConfig
  }

  def createCellAnnotationConfig(
      name: String,
      priority: Option[Int],
      fgColor: String,
      bgColor: String,
      displayName: MultiLanguageValue[String],
      isMultilang: Option[Boolean],
      isDashboard: Option[Boolean]
  )(implicit user: TableauxUser): Future[DomainObject] = {

    checkArguments(
      notNull(name, "name"),
      notNull(fgColor, "fgColor"),
      notNull(bgColor, "bgColor"),
      notNull(displayName, "displayName")
    )

    logger.info(
      s"createCellAnnotationConfig $name $priority $fgColor $bgColor $displayName $isMultilang $isDashboard"
    )

    for {
      _ <- roleModel.checkAuthorization(CreateCellAnnotationConfig)
      _ <- cellAnnotationConfigModel.create(name, priority, fgColor, bgColor, displayName, isMultilang, isDashboard)
      annotationConfig <- retrieveCellAnnotationConfig(name)
    } yield annotationConfig
  }
}
