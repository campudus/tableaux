package com.campudus.tableaux.api.system

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.JSONCompareMode
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class SystemServiceTest extends TableauxTestBase {

  @Test
  def retrieve_notExistingServices_throwsException(implicit c: TestContext): Unit =
    exceptionTest("NOT FOUND") {
      sendRequest("GET", "/system/services/1337")
    }

  @Test
  def retrieveAll_emptyServices_returnsEmptyArray(implicit c: TestContext): Unit = okTest {

    for {
      emptyServices <- sendRequest("GET", "/system/services")
    } yield {
      assertEquals(Json.emptyArr(), emptyServices.getJsonArray("services"))
    }
  }

  @Test
  def retrieve_insertedOneService_returnsService(implicit c: TestContext): Unit = okTest {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)

    val serviceJson = """
                        |{
                        |  "id": 1,
                        |  "type": "action",
                        |  "name": "service1",
                        |  "ordering": 1,
                        |  "displayName": {},
                        |  "description": { "en": "english", "de": "Deutsch" },
                        |  "active": true,
                        |  "updatedAt": null
                        |}""".stripMargin

    for {
      _ <- sqlConnection.query("""
                                 |INSERT INTO system_services
                                 |  ("type", "name", "ordering", "description", "active")
                                 |VALUES
                                 |  ('action', 'service1', 1, E'{"de": "Deutsch", "en": "english"}', TRUE)""".stripMargin)

      service1 <- sendRequest("GET", "/system/services/1")
    } yield {
      assertEqualsJSON(serviceJson, service1.toString, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def retrieveAll_insertedThreeServices_returnsAllService(implicit c: TestContext): Unit = okTest {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)

    val allServiceJson = """
                           |[
                           |  {
                           |    "id": 1,
                           |    "name": "service1"
                           |  }, {
                           |    "id": 2,
                           |    "name": "service2"
                           |  }, {
                           |    "id": 3,
                           |    "name": "service3"
                           |  }
                           |]""".stripMargin

    for {
      _ <- sqlConnection.query("""
                                 |INSERT INTO system_services
                                 |  ("type", "name", "ordering", "active")
                                 |VALUES
                                 |  ('action', 'service1', 1, TRUE),
                                 |  ('action', 'service2', 2, TRUE),
                                 |  ('action', 'service3', 3, TRUE)
                                 |  """.stripMargin)

      allServices <- sendRequest("GET", "/system/services").map(_.getJsonArray("services"))
    } yield {
      assertEquals(3, allServices.size())
      assertEqualsJSON(allServiceJson, allServices.toString, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def create_allPossibleFieldsProvided_returnsCreatedService(implicit c: TestContext): Unit = okTest {
    val serviceJson = """{
                        |  "name": "a new service",
                        |  "type": "action",
                        |  "ordering": 42,
                        |  "displayName": {
                        |    "de": "Mein erster Service",
                        |    "en": "My first service"
                        |  },
                        |  "description": {
                        |    "de": "super",
                        |    "en": "cool"
                        |  },
                        |  "active": true,
                        |  "config": {
                        |    "url": "https://any.customer.com",
                        |    "header": {
                        |      "API-Key": "1234"
                        |    }
                        |  },
                        |  "scope": {
                        |    "type": "table",
                        |    "tables": {
                        |      "includes": [
                        |        { "name": ".*_models" },
                        |        { "name": ".*_variants" }
                        |      ]
                        |    }
                        |  }
                        |}""".stripMargin

    for {
      service <- sendRequest("POST", "/system/services", serviceJson)
    } yield {
      assertEqualsJSON(serviceJson, service.toString, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def create_mandatoryFieldsProvided_returnsCreatedService(implicit c: TestContext): Unit = okTest {
    val serviceJson = """{
                        |  "name": "a new service",
                        |  "type": "action"
                        |}""".stripMargin
    for {
      service <- sendRequest("POST", "/system/services", serviceJson)
    } yield {
      assertEqualsJSON(serviceJson, service.toString, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def create_withoutName_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.arguments") {
      sendRequest("POST", "/system/services", """{ "type": "action" }""")
    }

  @Test
  def create_withoutType_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.arguments") {
      sendRequest("POST", "/system/services", """{ "name": "test service" }""")
    }

  @Test
  def create_invalidServiceType_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.arguments") {
      sendRequest("POST", "/system/services", """{ "type": "invalid_action", "name": "test service" }""")
    }

  @Test
  def delete_notExistingService_throwsException(implicit c: TestContext): Unit =
    exceptionTest("NOT FOUND") {
      sendRequest("DELETE", "/system/services/1337")
    }

  @Test
  def delete_existingService_returnsOk(implicit c: TestContext): Unit = okTest {
    val serviceJson = """{
                        |  "name": "a new service",
                        |  "type": "action"
                        |}""".stripMargin
    for {
      serviceId <- sendRequest("POST", "/system/services", serviceJson).map(_.getLong("id"))
      servicesBeforeDeletion <- sendRequest("GET", "/system/services").map(_.getJsonArray("services"))

      result <- sendRequest("DELETE", s"/system/services/$serviceId", serviceJson)

      servicesAfterDeletion <- sendRequest("GET", "/system/services").map(_.getJsonArray("services"))
    } yield {
      assertEquals(1, servicesBeforeDeletion.size)
      assertEqualsJSON("""{  "status": "ok" }""", result.toString)
      assertEquals(0, servicesAfterDeletion.size)
    }
  }
}
