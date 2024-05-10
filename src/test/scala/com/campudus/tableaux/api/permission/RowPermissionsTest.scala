package com.campudus.tableaux.api.permission

import com.campudus.tableaux.api.auth.AuthorizationTest
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

  val roleConfigWithViewRowPermissions =
    """
      |{
      |  "view-rows": [
      |    {
      |      "type": "grant",
      |      "action": ["viewColumn", "viewCellValue", "viewTable", "viewRow", "createRow"],
      |      "condition": {
      |        "row": {
      |          "permissions": "onlyGroupA|perm_1|perm_2|perm_3"
      |        }
      |      }
      |    }
      |  ]
      |}""".stripMargin

  val defaultViewTableRole =
    """
      |{
      |  "view-rows": [
      |    {
      |      "type": "grant",
      |      "action": ["viewColumn", "viewCellValue", "viewTable", "createRow"]
      |    }
      |  ]
      |}""".stripMargin

  def createTableauxController(roleConfig: String = defaultViewTableRole)(
      implicit roleModel: RoleModel = initRoleModel(roleConfig)
  ): TableauxController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val structureModel = StructureModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel)

    TableauxController(tableauxConfig, tableauxModel, roleModel)
  }
}

class foo {

  @Test
  def foo(): Unit = {
    assert(true)
  }
}

// @RunWith(classOf[VertxUnitRunner])
// class RowPermissionTest extends TableauxTestBase with TestHelper {

//   @Test
//   def replaceRowPermissions(implicit c: TestContext): Unit = {
//     okTest {
//       val controller = createTableauxController(roleConfigWithViewRowPermissions)
//       val expectedRow1 =
//         """
//           |{
//           |  "event": "row_permissions_changed",
//           |  "historyType": "row_permissions",
//           |  "valueType": "permissions",
//           |  "value": ["perm_1", "perm_2"]
//           |}
//         """.stripMargin

//       val expectedRow2 =
//         """
//           |{
//           |  "event": "row_permissions_changed",
//           |  "historyType": "row_permissions",
//           |  "valueType": "permissions",
//           |  "value": ["perm_3"]
//           |}
//         """.stripMargin

//       for {

//         _ <- createTableWithMultilanguageColumns("history test")
//         _ <- controller.createRow(1, None, None)
//         _ <- controller.replaceRowPermissions(1, 1, Seq("perm_1", "perm_2"))
//         _ <- controller.replaceRowPermissions(1, 1, Seq("perm_3"))

//         historyRows <- sendRequest("GET", "/tables/1/history?historyType=row_permissions").map(toRowsArray)
//         historyRow1 = historyRows.get[JsonObject](0)
//         historyRow2 = historyRows.get[JsonObject](1)

//         row <- controller.retrieveRow(1, 1)
//       } yield {
//         assertEquals(2, historyRows.size())
//         assertJSONEquals(expectedRow1, historyRow1.toString)
//         assertJSONEquals(expectedRow2, historyRow2.toString)
//         assertEquals(Seq("perm_3"), row.rowPermissions.value)
//       }
//     }
//   }

//   @Test
//   def addRowPermissions(implicit c: TestContext): Unit = {
//     okTest {
//       val controller = createTableauxController(roleConfigWithViewRowPermissions)
//       val expectedRow1 =
//         """
//           |{
//           |  "event": "row_permissions_changed",
//           |  "historyType": "row_permissions",
//           |  "valueType": "permissions",
//           |  "value": ["perm_1"]
//           |}
//         """.stripMargin

//       val expectedRow2 =
//         """
//           |{
//           |  "event": "row_permissions_changed",
//           |  "historyType": "row_permissions",
//           |  "valueType": "permissions",
//           |  "value": ["perm_1", "perm_2", "perm_3"]
//           |}
//         """.stripMargin

//       for {

//         _ <- createTableWithMultilanguageColumns("history test")
//         _ <- controller.createRow(1, None, None)
//         _ <- controller.addRowPermissions(1, 1, Seq("perm_1"))
//         _ <- controller.addRowPermissions(1, 1, Seq("perm_2", "perm_3", "perm_3"))

//         historyRows <- sendRequest("GET", "/tables/1/history?historyType=row_permissions").map(toRowsArray)

//         historyRow1 = historyRows.get[JsonObject](0)
//         historyRow2 = historyRows.get[JsonObject](1)
//         row <- controller.retrieveRow(1, 1)
//       } yield {
//         assertEquals(2, historyRows.size())
//         assertJSONEquals(expectedRow1, historyRow1.toString)
//         assertJSONEquals(expectedRow2, historyRow2.toString)
//         assertEquals(Seq("perm_1", "perm_2", "perm_3"), row.rowPermissions.value)
//       }
//     }
//   }

//   @Test
//   def deleteRowPermissions(implicit c: TestContext): Unit = {
//     okTest {
//       val controller = createTableauxController(roleConfigWithViewRowPermissions)
//       val expectedRow1 =
//         """
//           |{
//           |  "event": "row_permissions_changed",
//           |  "historyType": "row_permissions",
//           |  "valueType": "permissions",
//           |  "value": ["perm_1", "perm_2", "perm_3"]
//           |}
//         """.stripMargin

//       val expectedRow2 =
//         """
//           |{
//           |  "event": "row_permissions_changed",
//           |  "historyType": "row_permissions",
//           |  "valueType": "permissions",
//           |  "value": null
//           |}
//         """.stripMargin

//       for {

//         _ <- createTableWithMultilanguageColumns("history test")
//         _ <- controller.createRow(1, None, None)
//         _ <- controller.replaceRowPermissions(1, 1, Seq("perm_1", "perm_2", "perm_3"))
//         _ <- controller.deleteRowPermissions(1, 1)

//         historyRows <- sendRequest("GET", "/tables/1/history?historyType=row_permissions").map(toRowsArray)

//         historyRow1 = historyRows.get[JsonObject](0)
//         historyRow2 = historyRows.get[JsonObject](1)
//         row <- controller.retrieveRow(1, 1)
//       } yield {
//         assertEquals(2, historyRows.size())
//         assertJSONEquals(expectedRow1, historyRow1.toString)
//         assertJSONEquals(expectedRow2, historyRow2.toString)
//         assertEquals(Seq(), row.rowPermissions.value)
//       }
//     }
//   }

//   @Test
//   def addRowPermissionsWithCreateRow(implicit c: TestContext): Unit = {
//     okTest {
//       val controller = createTableauxController(roleConfigWithViewRowPermissions)
//       val expectedRow1 =
//         """
//           |{
//           |  "event": "row_permissions_changed",
//           |  "historyType": "row_permissions",
//           |  "valueType": "permissions",
//           |  "value": ["perm_1", "perm_2"]
//           |}
//         """.stripMargin

//       for {

//         _ <- createTableWithMultilanguageColumns("history test")
//         _ <- controller.createRow(1, None, Some(Seq("perm_1", "perm_2")))

//         historyRows <- sendRequest("GET", "/tables/1/history?historyType=row_permissions").map(toRowsArray)

//         historyRow1 = historyRows.get[JsonObject](0)
//         row <- controller.retrieveRow(1, 1)
//       } yield {
//         assertEquals(1, historyRows.size())
//         assertJSONEquals(expectedRow1, historyRow1.toString)
//         assertEquals(Seq("perm_1", "perm_2"), row.rowPermissions.value)
//       }
//     }
//   }

//   @Test
//   def addAndReplaceRowPermissions_ensureReadabilityWithNewRowPermissions_authorized(implicit c: TestContext): Unit = {
//     okTest {
//       val controller = createTableauxController(roleConfigWithViewRowPermissions)

//       for {
//         _ <- createTableWithMultilanguageColumns("history test")
//         _ <- controller.createRow(1, None, None)
//         _ <- controller.addRowPermissions(1, 1, Seq("perm_1"))
//         res <- controller.replaceRowPermissions(1, 1, Seq("perm_2"))
//       } yield {
//         assertNotNull(res)
//       }
//     }
//   }

//   @Test
//   def addRowPermissions_ensureReadabilityWithNewRowPermissions_unauthorized(implicit c: TestContext): Unit = {
//     exceptionTest("error.request.unauthorized") {
//       val controller = createTableauxController()

//       for {
//         _ <- createTableWithMultilanguageColumns("history test")
//         _ <- controller.createRow(1, None, None)
//         res <- controller.addRowPermissions(1, 1, Seq("perm_1"))
//       } yield {}
//     }
//   }

//   @Test
//   def replaceRowPermissions_ensureReadabilityWithNewRowPermissions_unauthorized(implicit c: TestContext): Unit = {
//     exceptionTest("error.request.unauthorized") {
//       val controller = createTableauxController()

//       for {
//         _ <- createTableWithMultilanguageColumns("history test")
//         _ <- controller.createRow(1, None, None)
//         res <- controller.replaceRowPermissions(1, 1, Seq("perm_1"))
//       } yield {}
//     }
//   }
// }

// @RunWith(classOf[VertxUnitRunner])
// class RetrieveRowsPermissionsTest extends AuthorizationTest with TestHelper {

//   @Test
//   def retrieveSingleRowWithPermissions_authorized(implicit c: TestContext): Unit = okTest {
//     val expectedJson: JsonObject = Json.obj("id" -> 1, "values" -> Json.arr("table1row1", 1))
//     val controller: TableauxController = createTableauxController(roleConfigWithViewRowPermissions)
//     for {
//       _ <- createDefaultTable()
//       _ <- sendRequest(
//         "PATCH",
//         "/tables/1/rows/1/permissions",
//         Json.obj("value" -> Json.arr("onlyGroupA")),
//         tokenWithRoles("view-tables", "view-test-row-permissions")
//       )

//       retrievedRow <- controller.retrieveRow(1, 1)
//     } yield {
//       assertJSONEquals(expectedJson, retrievedRow.getJson)
//     }
//   }

//   @Test
//   def retrieveSingleRowWithPermissions_unauthorized(implicit c: TestContext): Unit =
//     exceptionTest("error.request.unauthorized") {
//       val controller: TableauxController = createTableauxController()
//       for {
//         _ <- createDefaultTable()
//         _ <- sendRequest(
//           "PATCH",
//           "/tables/1/rows/1/permissions",
//           Json.obj("value" -> Json.arr("onlyGroupA", "onlyGroupB")),
//           tokenWithRoles("view-tables", "view-test-row-permissions")
//         )

//         retrievedRow <- controller.retrieveRow(1, 1)
//       } yield {}
//     }

//   @Test
//   def retrieveRowsWithPermissions_authorized(implicit c: TestContext): Unit = okTest {
//     val expectedJson: JsonArray = Json.arr(
//       Json.obj("id" -> 1, "values" -> Json.arr("table1row1", 1)),
//       Json.obj("id" -> 2, "values" -> Json.arr("table1row2", 2))
//     )
//     val controller: TableauxController = createTableauxController(roleConfigWithViewRowPermissions)
//     for {
//       _ <- createDefaultTable()
//       _ <-
//         sendRequest(
//           "PATCH",
//           "/tables/1/rows/1/permissions",
//           Json.obj("value" -> Json.arr("onlyGroupA", "onlyGroupB")),
//           tokenWithRoles("view-tables", "view-test-row-permissions")
//         )

//       retrievedRows <- controller.retrieveRows(1)
//     } yield {
//       assertJSONEquals(expectedJson, retrievedRows.getJson.getJsonArray("rows"))
//     }
//   }

//   @Test
//   def retrieveRowsWithPermissions_unauthorized(implicit c: TestContext): Unit = okTest {
//     val expectedJson: JsonArray = Json.arr(
//       Json.obj("id" -> 2, "values" -> Json.arr("table1row2", 2))
//     )
//     val controller: TableauxController = createTableauxController()
//     for {
//       _ <- createDefaultTable()
//       _ <-
//         sendRequest(
//           "PATCH",
//           "/tables/1/rows/1/permissions",
//           Json.obj("value" -> Json.arr("onlyGroupA", "onlyGroupB")),
//           tokenWithRoles("view-tables", "view-test-row-permissions")
//         )

//       retrievedRows <- controller.retrieveRows(1)
//     } yield {
//       assertJSONEquals(expectedJson, retrievedRows.getJson.getJsonArray("rows"))
//     }
//   }

//   @Test
//   def retrieveForeignRowsWithPermissions_authorized(implicit c: TestContext): Unit = okTest {
//     val expectedJson: JsonArray = Json.arr(
//       Json.obj("id" -> 1, "values" -> Json.arr("table1row1")),
//       Json.obj("id" -> 2, "values" -> Json.arr("table1row2"))
//     )
//     val createLinkColumn = Json.obj(
//       "columns" -> Json.arr(
//         Json.obj("name" -> "link col", "toTable" -> 1, "kind" -> "link")
//       )
//     )
//     val controller: TableauxController = createTableauxController(roleConfigWithViewRowPermissions)
//     for {
//       _ <- createDefaultTable()
//       _ <- createDefaultTable(" link test table", 2)
//       _ <- sendRequest(
//         "PATCH",
//         "/tables/1/rows/1/permissions",
//         Json.obj("value" -> Json.arr("onlyGroupA")),
//         tokenWithRoles("view-tables", "view-test-row-permissions")
//       )
//       _ <- sendRequest("POST", "/tables/2/columns", createLinkColumn)
//       _ <- sendRequest("PATCH", "/tables/2/columns/3/rows/1", Json.obj("value" -> 1))
//       _ <- sendRequest("PATCH", "/tables/2/columns/3/rows/1", Json.obj("value" -> 2))
//       cell <- sendRequest("GET", "/tables/2/columns/3/rows/1")
//       retrievedRows <- controller.retrieveForeignRows(2, 3, 1)
//     } yield {
//       assertEquals(expectedJson, retrievedRows.getJson.getJsonArray("rows"))
//     }
//   }

//   @Test
//   def retrieveForeignRowsWithPermissions_unauthorized(implicit c: TestContext): Unit = okTest {
//     val expectedJson: JsonArray = Json.arr(
//       Json.obj("id" -> 2, "values" -> Json.arr("table1row2"))
//     )
//     val createLinkColumn = Json.obj(
//       "columns" -> Json.arr(Json.obj("name" -> "link col", "toTable" -> 1, "kind" -> "link"))
//     )
//     val controller: TableauxController = createTableauxController()
//     for {
//       _ <- createDefaultTable()
//       _ <- createDefaultTable("link test table", 2)
//       _ <- sendRequest(
//         "PATCH",
//         "/tables/1/rows/1/permissions",
//         Json.obj("value" -> Json.arr("onlyGroupA")),
//         tokenWithRoles("view-tables", "view-test-row-permissions")
//       )
//       _ <- sendRequest("POST", "/tables/2/columns", createLinkColumn)
//       _ <- sendRequest("PATCH", "/tables/2/columns/3/rows/1", Json.obj("value" -> 1))
//       _ <- sendRequest("PATCH", "/tables/2/columns/3/rows/1", Json.obj("value" -> 2))
//       cell <- sendRequest("GET", "/tables/2/columns/3/rows/1")
//       retrievedRows <- controller.retrieveForeignRows(2, 3, 1)
//     } yield {
//       assertEquals(expectedJson, retrievedRows.getJson.getJsonArray("rows"))
//     }
//   }

//   @Test
//   def retrieveHiddenForeignRows_unauthorized(implicit c: TestContext): Unit = okTest {
//     val linkArray =
//       Json.arr(Json.obj("id" -> 1, "hiddenByRowPermissions" -> true), Json.obj("id" -> 2, "value" -> "table1row2"))
//     val row1 = Json.obj(
//       "id" -> 1,
//       "values" -> Json.arr(Json.arr("table2row1", 1, linkArray), "table2row1", 1, linkArray)
//     )
//     val row2 = Json.obj(
//       "id" -> 2,
//       "values" -> Json.arr(Json.arr("table2row2", 2, Json.emptyArr()), "table2row2", 2, Json.emptyArr())
//     )
//     val expectedJson: JsonArray = Json.arr(row1, row2)

//     val createLinkColumn = Json.obj(
//       "columns" -> Json.arr(Json.obj("name" -> "link col", "toTable" -> 1, "kind" -> "link"))
//     )
//     val identifierTrue = Json.obj("identifier" -> true)
//     val controller: TableauxController = createTableauxController()
//     for {
//       _ <- createDefaultTable()
//       _ <- createDefaultTable("link test table", 2)
//       _ <- sendRequest(
//         "PATCH",
//         "/tables/1/rows/1/permissions",
//         Json.obj("value" -> Json.arr("onlyGroupA")),
//         tokenWithRoles("view-tables", "view-test-row-permissions")
//       )

//       _ <- sendRequest("POST", "/tables/2/columns", createLinkColumn)
//       _ <- sendRequest("PATCH", "/tables/2/columns/2", identifierTrue)
//       _ <- sendRequest("PATCH", "/tables/2/columns/3", identifierTrue)
//       _ <- sendRequest("PATCH", "/tables/2/columns/3/rows/1", Json.obj("value" -> 1))
//       _ <- sendRequest("PATCH", "/tables/2/columns/3/rows/1", Json.obj("value" -> 2))
//       cell <- sendRequest("GET", "/tables/2/columns/3/rows/1")
//       retrievedRows <- controller.retrieveRows(2)
//     } yield {
//       val resultRows = retrievedRows.getJson.getJsonArray("rows")
//       assertEquals(expectedJson, resultRows)
//     }
//   }
// }
