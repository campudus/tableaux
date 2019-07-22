package com.campudus.tableaux.api.auth

import com.campudus.tableaux.UnauthorizedException
import com.campudus.tableaux.controller.SystemController
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.DomainObject
import com.campudus.tableaux.database.model.{ServiceModel, StructureModel, SystemModel, TableauxModel}
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.JSONCompareMode
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future

trait SystemControllerAuthTest extends TableauxTestBase {

  def createSystemController(implicit roleModel: RoleModel = RoleModel(Json.emptyObj())): SystemController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val systemModel = SystemModel(dbConnection)
    val structureModel = StructureModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel)
    val serviceModel = ServiceModel(dbConnection)

    SystemController(tableauxConfig, systemModel, tableauxModel, structureModel, serviceModel, roleModel)
  }

  private val simpleDefaultService: String = """{
                                               |  "name": "first service",
                                               |  "type": "action"
                                               |}""".stripMargin

  protected def createDefaultService(): Future[Long] =
    for {
      serviceId <- sendRequest("POST", "/system/services", simpleDefaultService).map(_.getLong("id"))
    } yield serviceId

  def getPermission(domainObject: DomainObject): JsonObject = {
    domainObject.getJson.getJsonObject("permission")
  }
}

@RunWith(classOf[VertxUnitRunner])
class SystemControllerAuthTest_enrichDomainObjects extends SystemControllerAuthTest {

  @Test
  def retrieveServices_createIsAllowed(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-create-services": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "create"],
                                    |      "scope": "service"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createSystemController(roleModel)

    for {
      serviceId <- createDefaultService()
      permission <- controller.retrieveServices().map(getPermission)
    } yield {
      val expected = Json.obj(
        "create" -> true
      )

      assertJSONEquals(expected, permission, JSONCompareMode.STRICT)
    }
  }

  @Test
  def retrieveServices_createIsNotAllowed(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-services": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "service"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createSystemController(roleModel)

    for {
      serviceId <- createDefaultService()
      permission <- controller.retrieveServices().map(getPermission)
    } yield {
      val expected = Json.obj(
        "create" -> false
      )

      assertJSONEquals(expected, permission, JSONCompareMode.STRICT)
    }
  }

}
