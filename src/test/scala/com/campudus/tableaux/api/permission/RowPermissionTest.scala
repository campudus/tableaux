package com.campudus.tableaux.api.permission

import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.Pagination
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
import org.scalatest.Ignore

trait TestHelper extends TableauxTestBase {

  def toRowsArray(obj: JsonObject): JsonArray = {
    obj.getJsonArray("rows")
  }

  def getLinksValue(arr: JsonArray, pos: Int): JsonArray = {
    arr.getJsonObject(pos).getJsonArray("value")
  }

  val defaultViewTableRole =
    """
      |{
      |  "view-rows": [
      |    {
      |      "type": "grant",
      |      "action": ["viewColumn", "viewCellValue", "viewTable"]
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
}

@RunWith(classOf[VertxUnitRunner])
class RowPermissionTest extends TableauxTestBase with TestHelper {

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
  def deleteRowPermissions(implicit c: TestContext): Unit = {
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
          |  "value": null
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
        _ <- sendRequest("DELETE", "/tables/1/rows/1/permissions")

        historyRows <- sendRequest("GET", "/tables/1/history?historyType=row_permissions").map(toRowsArray)

        historyRow1 = historyRows.get[JsonObject](0)
        historyRow2 = historyRows.get[JsonObject](1)
        row <- controller.retrieveRow(1, 1)
      } yield {
        assertEquals(2, historyRows.size())
        assertJSONEquals(expectedRow1, historyRow1.toString)
        assertJSONEquals(expectedRow2, historyRow2.toString)
        assertEquals(Seq(), row.rowPermissions.value)
      }
    }
  }

  @Test
  def addRowPermissionsWithCreateRow(implicit c: TestContext): Unit = {
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

      for {

        _ <- createTableWithMultilanguageColumns("history test")
        _ <- sendRequest("POST", "/tables/1/rows", Json.obj("rowPermissions" -> Json.arr("perm_1", "perm_2")))

        historyRows <- sendRequest("GET", "/tables/1/history?historyType=row_permissions").map(toRowsArray)

        historyRow1 = historyRows.get[JsonObject](0)
        row <- controller.retrieveRow(1, 1)
      } yield {
        assertEquals(1, historyRows.size())
        assertJSONEquals(expectedRow1, historyRow1.toString)
        assertEquals(Seq("perm_1", "perm_2"), row.rowPermissions.value)
      }
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class RetrieveRowsPermissionsTest extends TableauxTestBase with TestHelper {

  @Test
  def retrieveRowWithPermissions_authorized(implicit c: TestContext): Unit = okTest {
    val expectedJson: JsonObject = Json.obj(
      "id" -> 1,
      "values" -> Json.arr(
        "table1row1",
        1
      )
    )
    val controller: TableauxController = createTableauxController()
    for {
      _ <- createDefaultTable()
      _ <- sendRequest("PATCH", "/tables/1/rows/1/permissions", Json.obj("value" -> Json.arr("onlyGroupA")))

      retrievedRow <- controller.retrieveRow(1, 1)
    } yield {
      assertJSONEquals(expectedJson, retrievedRow.getJson)
    }
  }

  @Test
  def retrieveRowWithPermissions_unauthorized(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val expectedJson: JsonObject = Json.obj(
        "id" -> 1,
        "values" -> Json.arr(
          "table1row1",
          1
        )
      )
      val controller: TableauxController = createTableauxController()
      for {
        _ <- createDefaultTable()
        _ <- sendRequest("PATCH", "/tables/1/rows/1/permissions", Json.obj("value" -> Json.arr("onlyGroupA")))

        retrievedRow <- controller.retrieveRow(1, 1)
      } yield {}
    }

  @Test
  def retrieveRowsWithPermissions_authorized(implicit c: TestContext): Unit = okTest {
    val expectedJson: JsonArray = Json.arr(
      Json.obj(
        "id" -> 1,
        "values" -> Json.arr(
          "table1row1",
          1
        )
      ),
      Json.obj(
        "id" -> 2,
        "values" -> Json.arr(
          "table1row2",
          2
        )
      )
    )
    val controller: TableauxController = createTableauxController()
    for {
      _ <- createDefaultTable()
      _ <- sendRequest("PATCH", "/tables/1/rows/1/permissions", Json.obj("value" -> Json.arr("onlyGroupA")))

      retrievedRows <- controller.retrieveRows(1, Pagination(None, None))
    } yield {
      assertJSONEquals(expectedJson, retrievedRows.getJson.getJsonArray("rows"))
    }
  }

  @Test
  def retrieveRowsWithPermissions_unauthorized(implicit c: TestContext): Unit = okTest {
    val expectedJson: JsonArray = Json.arr(
      Json.obj(
        "id" -> 2,
        "values" -> Json.arr(
          "table1row2",
          2
        )
      )
    )
    val controller: TableauxController = createTableauxController()
    for {
      _ <- createDefaultTable()
      _ <- sendRequest("PATCH", "/tables/1/rows/1/permissions", Json.obj("value" -> Json.arr("onlyGroupA")))

      retrievedRows <- controller.retrieveRows(1, Pagination(None, None))
    } yield {
      assertJSONEquals(expectedJson, retrievedRows.getJson.getJsonArray("rows"))
    }
  }

  @Test
  def retrieveForeignRowsWithPermissions_authorized(implicit c: TestContext): Unit = okTest {
    val expectedJson: JsonArray = Json.arr(
      Json.obj(
        "id" -> 1,
        "values" -> Json.arr(
          "table1row1"
        )
      ),
      Json.obj(
        "id" -> 2,
        "values" -> Json.arr(
          "table1row2"
        )
      )
    )
    val createLinkColumn = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "link col",
          "toTable" -> 1,
          "kind" -> "link"
        )
      )
    )
    val controller: TableauxController = createTableauxController()
    for {
      _ <- createDefaultTable()
      _ <- createDefaultTable(" link test table", 2)
      _ <- sendRequest("PATCH", "/tables/1/rows/1/permissions", Json.obj("value" -> Json.arr("onlyGroupA")))
      _ <- sendRequest("POST", "/tables/2/columns", createLinkColumn)
      _ <- sendRequest("PATCH", "/tables/2/columns/3/rows/1", Json.obj("value" -> 1))
      _ <- sendRequest("PATCH", "/tables/2/columns/3/rows/1", Json.obj("value" -> 2))
      cell <- sendRequest("GET", "/tables/2/columns/3/rows/1")
      retrievedRows <- controller.retrieveForeignRows(2, 3, 1, Pagination(None, None))
    } yield {
      assertEquals(expectedJson, retrievedRows.getJson.getJsonArray("rows"))
    }
  }

  @Test
  def retrieveForeignRowsWithPermissions_unauthorized(implicit c: TestContext): Unit = okTest {
    val expectedJson: JsonArray = Json.arr(
      Json.obj(
        "id" -> 2,
        "values" -> Json.arr(
          "table1row2"
        )
      )
    )
    val createLinkColumn = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "link col",
          "toTable" -> 1,
          "kind" -> "link"
        )
      )
    )
    val controller: TableauxController = createTableauxController()
    for {
      _ <- createDefaultTable()
      _ <- createDefaultTable(" link test table", 2)
      _ <- sendRequest("PATCH", "/tables/1/rows/1/permissions", Json.obj("value" -> Json.arr("onlyGroupA")))
      _ <- sendRequest("POST", "/tables/2/columns", createLinkColumn)
      _ <- sendRequest("PATCH", "/tables/2/columns/3/rows/1", Json.obj("value" -> 1))
      _ <- sendRequest("PATCH", "/tables/2/columns/3/rows/1", Json.obj("value" -> 2))
      cell <- sendRequest("GET", "/tables/2/columns/3/rows/1")
      retrievedRows <- controller.retrieveForeignRows(2, 3, 1, Pagination(None, None))
    } yield {
      assertEquals(expectedJson, retrievedRows.getJson.getJsonArray("rows"))
    }
  }

  @Test
  def createRowAndSetPermissionFlagsForSingleRow(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId <- createEmptyDefaultTable()

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest(
          "PATCH",
          s"/tables/$tableId/rows/$rowId/annotations",
          Json.obj("permissions" -> Json.arr("perm_1", "perm_2"))
        )

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

        _ <- sendRequest("PATCH", s"/tables/$tableId/rows/$rowId/annotations", Json.obj("permissions" -> Json.arr()))

        rowJson2 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        val expectedRowJson1 = Json.obj(
          "status" -> "ok",
          "id" -> rowId,
          "permissions" -> Json.arr("perm_1", "perm_2"),
          "values" -> Json.arr(null, null)
        )

        assertJSONEquals(expectedRowJson1, rowJson1)

        val expectedRowJson2 = Json.obj(
          "status" -> "ok",
          "id" -> rowId,
          "permissions" -> Json.arr(),
          "values" -> Json.arr(null, null)
        )

        assertJSONEquals(expectedRowJson2, rowJson2)
      }
    }
  }
}
