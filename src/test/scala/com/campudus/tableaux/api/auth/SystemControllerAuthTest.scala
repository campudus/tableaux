package com.campudus.tableaux.api.auth

import com.campudus.tableaux.UnauthorizedException
import com.campudus.tableaux.controller.SystemController
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.{DomainObject, MultiLanguageValue, ServiceTypeAction}
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

  private def simpleDefaultService(name: String): String = s"""{
                                                              |  "name": "$name",
                                                              |  "type": "action"
                                                              |}""".stripMargin

  protected def createDefaultService(name: String = "first service"): Future[Long] =
    for {
      serviceId <- sendRequest("POST", "/system/services", simpleDefaultService(name)).map(_.getLong("id"))
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

  @Test
  def retrieveService_allActionsAreAllowed(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-and-delete-services": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editDisplayProperty", "editStructureProperty", "delete"],
                                    |      "scope": "service"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createSystemController(roleModel)

    for {
      serviceId <- createDefaultService()
      permission <- controller.retrieveService(serviceId).map(getPermission)
    } yield {
      val expected = Json.obj(
        "editDisplayProperty" -> true,
        "editStructureProperty" -> true,
        "delete" -> true
      )

      assertJSONEquals(expected, permission, JSONCompareMode.STRICT)
    }
  }

  @Test
  def retrieveService_noActionIsAllowed(implicit c: TestContext): Unit = okTest {
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
      permission <- controller.retrieveService(serviceId).map(getPermission)
    } yield {
      val expected = Json.obj(
        "editDisplayProperty" -> false,
        "editStructureProperty" -> false,
        "delete" -> false
      )

      assertJSONEquals(expected, permission, JSONCompareMode.STRICT)
    }
  }

}

@RunWith(classOf[VertxUnitRunner])
class SystemControllerAuthTest_checkAuthorization extends SystemControllerAuthTest {

  @Test
  def createService_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "create-services": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "create"],
                                    |      "scope": "service"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createSystemController(roleModel)

    for {
      permission <- controller
        .createService("a service",
                       ServiceTypeAction,
                       None,
                       MultiLanguageValue[String](None),
                       MultiLanguageValue[String](None),
                       true,
                       None,
                       None)

    } yield ()
  }

  @Test
  def createService_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "create-services": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "service"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createSystemController(roleModel)

    for {
      ex <- controller
        .createService("a service",
                       ServiceTypeAction,
                       None,
                       MultiLanguageValue[String](None),
                       MultiLanguageValue[String](None),
                       true,
                       None,
                       None)
        .recover({ case ex => ex })

    } yield {
      assertEquals(UnauthorizedException(Create, ScopeService), ex)
    }
  }

  @Test
  def deleteService_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "create-services": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "delete"],
                                    |      "scope": "service"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createSystemController(roleModel)

    for {
      serviceId <- createDefaultService()
      _ <- controller.deleteService(serviceId)
    } yield ()
  }

  @Test
  def deleteService_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "create-services": [
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
      ex <- controller.deleteService(serviceId).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(Delete, ScopeService), ex)
    }
  }

  @Test
  def retrieveService_authorized_ok(implicit c: TestContext): Unit = okTest {
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
      _ <- controller.retrieveService(serviceId)
    } yield ()
  }

  @Test
  def retrieveService_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val controller = createSystemController()

    for {
      serviceId <- createDefaultService()
      ex <- controller.retrieveService(serviceId).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(View, ScopeService), ex)
    }
  }

  @Test
  def updateServiceEditStructureProperty_authorized_ok(implicit c: TestContext): Unit = okTest {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-services": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editStructureProperty"],
                                    |      "scope": "service"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createSystemController(roleModel)

    for {
      serviceId <- createDefaultService()
      _ <- controller.updateService(serviceId, Some("changed name"), None, None, None, None, None, None, None)
    } yield ()
  }

  @Test
  def updateServiceEditStructureProperty_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-services": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editDisplayProperty"],
                                    |      "scope": "service"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createSystemController(roleModel)

    for {
      serviceId <- createDefaultService()
      ex <- controller
        .updateService(serviceId, Some("changed name"), None, None, None, None, None, None, None)
        .recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(EditStructureProperty, ScopeService), ex)
    }
  }

  @Test
  def updateServiceEditDisplayProperties_authorized_ok(implicit c: TestContext): Unit = okTest {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-services": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editDisplayProperty"],
                                    |      "scope": "service"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createSystemController(roleModel)

    for {
      serviceId <- createDefaultService()
      _ <- controller.updateService(serviceId, None, None, Some(10), None, None, Some(true), None, None)
    } yield ()
  }

}

@RunWith(classOf[VertxUnitRunner])
class SystemControllerAuthTest_filterAuthorization extends SystemControllerAuthTest {

  @Test
  def retrieveServices_authorized_ok(implicit c: TestContext): Unit = okTest {
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
      serviceId1 <- createDefaultService("first service")
      serviceId2 <- createDefaultService("second service")
      services <- controller.retrieveServices()
    } yield {
      assertEquals(2, services.services.length)
    }
  }

  @Test
  def retrieveServices_noViewPermission_returnEmptyList(implicit c: TestContext): Unit = okTest {
    val controller = createSystemController()

    for {
      serviceId1 <- createDefaultService("first service")
      serviceId2 <- createDefaultService("second service")
      services <- controller.retrieveServices()
    } yield {
      assertEquals(0, services.services.length)
    }
  }

}
