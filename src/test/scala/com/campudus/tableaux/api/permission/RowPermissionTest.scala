package com.campudus.tableaux.api.permission

import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.model.SystemModel
import com.campudus.tableaux.database.model.TableauxModel
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.campudus.tableaux.testtools.{RequestCreation, TableauxTestBase}
import com.campudus.tableaux.testtools.RequestCreation.{Identifier, TextCol}

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.vertx.scala.core.json._
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

trait TestHelper {

  def toRowsArray(obj: JsonObject): JsonArray = {
    obj.getJsonArray("rows")
  }

  def getLinksValue(arr: JsonArray, pos: Int): JsonArray = {
    arr.getJsonObject(pos).getJsonArray("value")
  }

}

@RunWith(classOf[VertxUnitRunner])
class RowPermissionTest extends TableauxTestBase with TestHelper {

  val defaultViewTableRole = """
                               |{
                               |  "view-all-tables": [
                               |    {
                               |      "type": "grant",
                               |      "action": ["viewTable"]
                               |    }
                               |  ]
                               |}""".stripMargin

  def createTableauxController(
      implicit roleModel: RoleModel = initRoleModel(defaultViewTableRole)
  ): TableauxController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val structureModel = StructureModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel)

    TableauxController(tableauxConfig, tableauxModel, roleModel)
  }

  @Test
  def replaceRowPermissions(implicit c: TestContext): Unit = {
    okTest {
      val controller = createTableauxController()
      val expectedRow1 =
        """
          |{
          |  "event": "row_permissions_changed",
          |  "historyType": "row_permissions",
          |  "valueType": "permissions",
          |  "value": ["perm_1", "perm_2"]
          |}
        """.stripMargin

      val expectedRow2 =
        """
          |{
          |  "event": "row_permissions_changed",
          |  "historyType": "row_permissions",
          |  "valueType": "permissions",
          |  "value": ["perm_3"]
          |}
        """.stripMargin

      for {

        _ <- createTableWithMultilanguageColumns("history test")
        _ <- sendRequest("POST", "/tables/1/rows")

        _ <-
          sendRequest("PUT", "/tables/1/rows/1/permissions", Json.obj("value" -> Json.arr("perm_1", "perm_2")))
        _ <- sendRequest("PUT", "/tables/1/rows/1/permissions", Json.obj("value" -> Json.arr("perm_3")))

        historyRows <- sendRequest("GET", "/tables/1/history?historyType=row_permissions").map(toRowsArray)
        historyRow1 = historyRows.get[JsonObject](0)
        historyRow2 = historyRows.get[JsonObject](1)

        row <- controller.retrieveRow(1, 1)
      } yield {
        assertEquals(2, historyRows.size())
        assertJSONEquals(expectedRow1, historyRow1.toString)
        assertJSONEquals(expectedRow2, historyRow2.toString)
        assertEquals(Seq("perm_3"), row.rowPermissions.value)
      }
    }
  }

  @Test
  def addRowPermissions(implicit c: TestContext): Unit = {
    okTest {
      val controller = createTableauxController()
      val expectedRow1 =
        """
          |{
          |  "event": "row_permissions_changed",
          |  "historyType": "row_permissions",
          |  "valueType": "permissions",
          |  "value": ["perm_1"]
          |}
        """.stripMargin

      val expectedRow2 =
        """
          |{
          |  "event": "row_permissions_changed",
          |  "historyType": "row_permissions",
          |  "valueType": "permissions",
          |  "value": ["perm_1", "perm_2", "perm_3"]
          |}
        """.stripMargin

      for {

        _ <- createTableWithMultilanguageColumns("history test")
        _ <- sendRequest("POST", "/tables/1/rows")

        _ <- sendRequest("PUT", "/tables/1/rows/1/permissions", Json.obj("value" -> Json.arr("perm_1")))
        _ <- sendRequest(
          "PATCH",
          "/tables/1/rows/1/permissions",
          Json.obj("value" -> Json.arr("perm_2", "perm_3", "perm_3"))
        )

        historyRows <- sendRequest("GET", "/tables/1/history?historyType=row_permissions").map(toRowsArray)

        historyRow1 = historyRows.get[JsonObject](0)
        historyRow2 = historyRows.get[JsonObject](1)
        row <- controller.retrieveRow(1, 1)
      } yield {
        assertEquals(2, historyRows.size())
        assertJSONEquals(expectedRow1, historyRow1.toString)
        assertJSONEquals(expectedRow2, historyRow2.toString)
        assertEquals(Seq("perm_1", "perm_2", "perm_3"), row.rowPermissions.value)
      }
    }
  }

  @Test
  def removeRowPermissions(implicit c: TestContext): Unit = {
    okTest {
      val controller = createTableauxController()
      val expectedRow1 =
        """
          |{
          |  "event": "row_permissions_changed",
          |  "historyType": "row_permissions",
          |  "valueType": "permissions",
          |  "value": ["perm_1", "perm_2", "perm_3"]
          |}
        """.stripMargin

      val expectedRow2 =
        """
          |{
          |  "event": "row_permissions_changed",
          |  "historyType": "row_permissions",
          |  "valueType": "permissions",
          |  "value": ["perm_2"]
          |}
        """.stripMargin

      for {

        _ <- createTableWithMultilanguageColumns("history test")
        _ <- sendRequest("POST", "/tables/1/rows")

        _ <- sendRequest(
          "PUT",
          "/tables/1/rows/1/permissions",
          Json.obj("value" -> Json.arr("perm_1", "perm_2", "perm_3"))
        )
        _ <- sendRequest(
          "DELETE",
          "/tables/1/rows/1/permissions",
          Json.obj("value" -> Json.arr("perm_1", "perm_3", "perm_3", "perm_4"))
        )

        historyRows <- sendRequest("GET", "/tables/1/history?historyType=row_permissions").map(toRowsArray)

        historyRow1 = historyRows.get[JsonObject](0)
        historyRow2 = historyRows.get[JsonObject](1)
        row <- controller.retrieveRow(1, 1)
      } yield {
        assertEquals(2, historyRows.size())
        assertJSONEquals(expectedRow1, historyRow1.toString)
        assertJSONEquals(expectedRow2, historyRow2.toString)
        assertEquals(Seq("perm_2"), row.rowPermissions.value)
      }
    }
  }
}
