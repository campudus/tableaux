package com.campudus.tableaux.api.content

import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.Pagination
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.model.TableauxModel
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.core.json.JsonArray
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import io.vertx.scala.ext.sql
import org.vertx.scala.core.json.{Json, JsonObject}

import org.junit.{Ignore, Test}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.JSONCompareMode

@RunWith(classOf[VertxUnitRunner])
class RetrieveTest extends TableauxTestBase {

  val createTableJson: JsonObject = Json.obj("name" -> "Test Table 1")

  val createStringColumnJson: JsonObject =
    Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))

  val createNumberColumnJson: JsonObject =
    Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))

  val createBooleanColumnJson: JsonObject =
    Json.obj("columns" -> Json.arr(Json.obj("kind" -> "boolean", "name" -> "Test Column 3")))

  @Test
  def retrieveCompleteTableWhichIsEmpty(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "columns" -> Json.arr(),
      "page" -> Json.obj(
        "offset" -> null,
        "limit" -> null,
        "totalSize" -> 0
      ),
      "attributes" -> Json.obj(),
      "rows" -> Json.arr(),
      "langtags" -> Json.arr("de-DE", "en-GB"),
      "permission" -> Json.obj(
        "editDisplayProperty" -> true,
        "editStructureProperty" -> true,
        "delete" -> true,
        "createRow" -> true,
        "deleteRow" -> true,
        "editCellAnnotation" -> true,
        "editRowAnnotation" -> true
      )
    )

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test <- sendRequest("GET", "/completetable/1")
    } yield {
      assertJSONEquals(expectedJson, test, JSONCompareMode.STRICT)
    }
  }

  @Test
  def retrieveCompleteTableWithNoRows(implicit c: TestContext): Unit = {
    okTest {
      val expectedJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "Test Table 1",
        "hidden" -> false,
        "displayName" -> Json.obj(),
        "description" -> Json.obj(),
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 1,
            "name" -> "Test Column 1",
            "kind" -> "text",
            "ordering" -> 1,
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj()
          ),
          Json.obj(
            "id" -> 2,
            "name" -> "Test Column 2",
            "kind" -> "numeric",
            "ordering" -> 2,
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj()
          ),
          Json.obj(
            "id" -> 3,
            "name" -> "Test Column 3",
            "kind" -> "boolean",
            "ordering" -> 3,
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj()
          )
        ),
        "page" -> Json.obj(
          "offset" -> null,
          "limit" -> null,
          "totalSize" -> 0
        ),
        "rows" -> Json.arr(),
        "langtags" -> Json.arr("de-DE", "en-GB")
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
        _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
        _ <- sendRequest("POST", "/tables/1/columns", createBooleanColumnJson)
        test <- sendRequest("GET", "/completetable/1")
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }

  @Test
  def retrieveCompleteTableWithRows(implicit c: TestContext): Unit = {
    okTest {
      val expectedJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "Test Table 1",
        "hidden" -> false,
        "displayName" -> Json.obj(),
        "description" -> Json.obj(),
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 1,
            "name" -> "Test Column 1",
            "kind" -> "text",
            "ordering" -> 1,
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj()
          ),
          Json.obj(
            "id" -> 2,
            "name" -> "Test Column 2",
            "kind" -> "numeric",
            "ordering" -> 2,
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj()
          ),
          Json.obj(
            "id" -> 3,
            "name" -> "Test Column 3",
            "kind" -> "boolean",
            "ordering" -> 3,
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj()
          )
        ),
        "page" -> Json.obj(
          "offset" -> null,
          "limit" -> null,
          "totalSize" -> 1
        ),
        "rows" -> Json.arr(Json.obj("id" -> 1, "values" -> Json.arr(null, null, false))),
        "langtags" -> Json.arr("de-DE", "en-GB")
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
        _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
        _ <- sendRequest("POST", "/tables/1/columns", createBooleanColumnJson)
        _ <- sendRequest("POST", "/tables/1/rows")
        test <- sendRequest("GET", "/completetable/1")
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }

  @Test
  def retrieveCompleteTable(implicit c: TestContext): Unit = {
    okTest {
      val columns = Json.arr(
        Json.obj(
          "id" -> 1,
          "name" -> "Test Column 1",
          "kind" -> "text",
          "ordering" -> 1,
          "multilanguage" -> false,
          "identifier" -> false,
          "displayName" -> Json.obj(),
          "description" -> Json.obj()
        ),
        Json.obj(
          "id" -> 2,
          "name" -> "Test Column 2",
          "kind" -> "numeric",
          "ordering" -> 2,
          "multilanguage" -> false,
          "identifier" -> false,
          "displayName" -> Json.obj(),
          "description" -> Json.obj()
        ),
        Json.obj(
          "id" -> 3,
          "name" -> "Test Column 3",
          "kind" -> "boolean",
          "ordering" -> 3,
          "multilanguage" -> false,
          "identifier" -> false,
          "displayName" -> Json.obj(),
          "description" -> Json.obj()
        )
      )

      val rows = Json.arr(
        Json.obj("id" -> 1, "values" -> Json.arr("table1row1", 1, false)),
        Json.obj("id" -> 2, "values" -> Json.arr("table1row2", 2, true)),
        Json.obj("id" -> 3, "values" -> Json.arr("table1row3", 3, false))
      )

      val expectedJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "Test Table 1",
        "hidden" -> false,
        "displayName" -> Json.obj(),
        "description" -> Json.obj(),
        "columns" -> columns,
        "page" -> Json.obj(
          "offset" -> null,
          "limit" -> null,
          "totalSize" -> 3
        ),
        "rows" -> rows,
        "langtags" -> Json.arr("de-DE", "en-GB")
      )

      def valuesObj(values: Any*): JsonObject = {
        Json.obj("values" -> values)
      }

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
        _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
        _ <- sendRequest("POST", "/tables/1/columns", createBooleanColumnJson)
        _ <- sendRequest(
          "POST",
          "/tables/1/rows",
          Json.obj("columns" -> columns, "rows" -> Json.arr(Json.obj("values" -> Json.arr("table1row1", 1, false))))
        )
        _ <- sendRequest(
          "POST",
          "/tables/1/rows",
          Json.obj("columns" -> columns, "rows" -> Json.arr(Json.obj("values" -> Json.arr("table1row2", 2, true))))
        )
        _ <- sendRequest(
          "POST",
          "/tables/1/rows",
          Json.obj("columns" -> columns, "rows" -> Json.arr(Json.obj("values" -> Json.arr("table1row3", 3, false))))
        )
        test <- sendRequest("GET", "/completetable/1")
      } yield {
        assertJSONEquals(expectedJson, test)
      }
    }
  }

}

@RunWith(classOf[VertxUnitRunner])
class RetrieveRowsTest extends TableauxTestBase {

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
  def retrieveRow(implicit c: TestContext): Unit = okTest {
    val expectedJson =
      Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "values" -> Json.arr("table1row1", 1)
      )

    for {
      _ <- createDefaultTable()
      test <- sendRequest("GET", "/tables/1/rows/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveRows(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "page" -> Json.obj(
        "offset" -> null,
        "limit" -> null,
        "totalSize" -> 2
      ),
      "rows" -> Json.arr(
        Json.obj("id" -> 1, "values" -> Json.arr("table1row1", 1)),
        Json.obj("id" -> 2, "values" -> Json.arr("table1row2", 2))
      )
    )

    for {
      _ <- createDefaultTable()
      test <- sendRequest("GET", "/tables/1/rows")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrievePagedRows(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "page" -> Json.obj(
        "offset" -> 1,
        "limit" -> 1,
        "totalSize" -> 2
      ),
      "rows" -> Json.arr(
        Json.obj("id" -> 2, "values" -> Json.arr("table1row2", 2))
      )
    )

    for {
      _ <- createDefaultTable()
      test <- sendRequest("GET", "/tables/1/rows?offset=1&limit=1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveRows_offsetIsLowerZero_returns422(implicit c: TestContext): Unit = {
    exceptionTest("error.arguments") {
      for {
        _ <- createDefaultTable()
        _ <- sendRequest("GET", "/tables/1/rows?offset=-1")
      } yield ()
    }
  }

  @Test
  def retrieveRows_limitIsLowerZero_returns422(implicit c: TestContext): Unit = {
    exceptionTest("error.arguments") {
      for {
        _ <- createDefaultTable()
        _ <- sendRequest("GET", "/tables/1/rows?limit=-1")
      } yield ()
    }
  }

  @Test
  def retrieveRowsOfSpecificColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "page" -> Json.obj(
        "offset" -> null,
        "limit" -> null,
        "totalSize" -> 2
      ),
      "rows" -> Json.arr(
        Json.obj("id" -> 1, "values" -> Json.arr(1)),
        Json.obj("id" -> 2, "values" -> Json.arr(2))
      )
    )

    for {
      _ <- createDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/2/rows")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveRowsOfConcatColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.arr(
      Json.obj("id" -> 1, "values" -> Json.arr(Json.arr(1, "table1row1"))),
      Json.obj("id" -> 2, "values" -> Json.arr(Json.arr(2, "table1row2")))
    )

    for {
      _ <- createDefaultTable()
      _ <- sendRequest("POST", "/tables/1/columns/2", Json.obj("ordering" -> 0, "identifier" -> true))

      test <- sendRequest("GET", "/tables/1/columns/0/rows")
    } yield {
      assertEquals(expectedJson, test.getJsonArray("rows"))
    }
  }

  @Test
  def retrieveRowsOfFirstColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.arr(
      Json.obj("id" -> 1, "values" -> Json.arr("table1row1")),
      Json.obj("id" -> 2, "values" -> Json.arr("table1row2"))
    )

    for {
      _ <- createDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/first/rows")
    } yield {
      assertEquals(expectedJson, test.getJsonArray("rows"))
    }
  }

  @Test
  def retrieveRowsOfFirstColumnWithConcatColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.arr(
      Json.obj("id" -> 1, "values" -> Json.arr(Json.arr(1, "table1row1"))),
      Json.obj("id" -> 2, "values" -> Json.arr(Json.arr(2, "table1row2")))
    )

    for {
      _ <- createDefaultTable()
      _ <- sendRequest("POST", "/tables/1/columns/2", Json.obj("ordering" -> 0, "identifier" -> true))

      test <- sendRequest("GET", "/tables/1/columns/first/rows")
    } yield {
      assertEquals(expectedJson, test.getJsonArray("rows"))
    }
  }

}

@RunWith(classOf[VertxUnitRunner])
class RetrieveCellTest extends TableauxTestBase {

  @Test
  def retrieveCell(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "value" -> "table1row1")
    val expectedJson2 = Json.obj("status" -> "ok", "value" -> 1)

    for {
      _ <- createDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/1/rows/1")
      test2 <- sendRequest("GET", "/tables/1/columns/2/rows/1")
    } yield {
      assertEquals(expectedJson, test)
      assertEquals(expectedJson2, test2)
    }
  }
}
