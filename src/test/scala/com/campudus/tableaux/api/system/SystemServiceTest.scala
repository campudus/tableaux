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
  def retrieve_notExistingServices_throwException(implicit c: TestContext): Unit =
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
                        |  "active": true
                        |}""".stripMargin

//                        TODO add config + scope to response
//                        |  "config": {
//                        |    "url": "https://any.customer.com",
//                        |    "header": {
//                        |      "API-Key": "1234"
//                        |    }
//                        |  },
//                        |  "scope": {
//                        |    "type": "table",
//                        |    "tables": {
//                        |      "includes": [
//                        |        { "name": ".*_models" },
//                        |        { "name": ".*_variants" }
//                        |      ]
//                        |    }
//                        |  }

    for {
      service <- sendRequest("POST", "/system/services", serviceJson)
    } yield {
      assertEqualsJSON(serviceJson, service.toString, JSONCompareMode.LENIENT)
    }
  }
}
