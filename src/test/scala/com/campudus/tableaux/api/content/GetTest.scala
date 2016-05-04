package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class GetTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Table 1")
  val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))
  val createNumberColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))
  val createBooleanColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "boolean", "name" -> "Test Column 3")))

  @Test
  def retrieveEmptyTable(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "hidden" -> false,
      "columns" -> Json.arr(),
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
      test <- sendRequest("GET", "/completetable/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveWithColumnTable(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "hidden" -> false,
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()),
        Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()),
        Json.obj("id" -> 3, "name" -> "Test Column 3", "kind" -> "boolean", "ordering" -> 3, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj())),
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
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveWithColumnAndRowTable(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "hidden" -> false,
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()),
        Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()),
        Json.obj("id" -> 3, "name" -> "Test Column 3", "kind" -> "boolean", "ordering" -> 3, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj())),
      "page" -> Json.obj(
        "offset" -> null,
        "limit" -> null,
        "totalSize" -> 1
      ),
      "rows" -> Json.arr(
        Json.obj("id" -> 1, "values" -> Json.arr(null, null, null))),
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
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveFullTable(implicit c: TestContext): Unit = okTest {
    val columns = Json.arr(
      Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()),
      Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj()),
      Json.obj("id" -> 3, "name" -> "Test Column 3", "kind" -> "boolean", "ordering" -> 3, "multilanguage" -> false, "identifier" -> false, "displayName" -> Json.obj(), "description" -> Json.obj())
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
      _ <- sendRequest("POST", "/tables/1/rows", Json.obj("columns" -> columns, "rows" -> Json.arr(Json.obj("values" -> Json.arr("table1row1", 1, false)))))
      _ <- sendRequest("POST", "/tables/1/rows", Json.obj("columns" -> columns, "rows" -> Json.arr(Json.obj("values" -> Json.arr("table1row2", 2, true)))))
      _ <- sendRequest("POST", "/tables/1/rows", Json.obj("columns" -> columns, "rows" -> Json.arr(Json.obj("values" -> Json.arr("table1row3", 3, false)))))
      test <- sendRequest("GET", "/completetable/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveRow(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 1, "values" -> Json.arr("table1row1", 1))

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
    val expectedJsonAfterOrdering = Json.arr(
      Json.obj("id" -> 1, "values" -> Json.arr(Json.arr(1, "table1row1"))),
      Json.obj("id" -> 2, "values" -> Json.arr(Json.arr(2, "table1row2")))
    )

    for {
      _ <- createDefaultTable()
      _ <- sendRequest("GET", "/tables/1/columns/first/rows")

      _ <- sendRequest("POST", "/tables/1/columns/2", Json.obj("ordering" -> 0, "identifier" -> true))

      testAfterOrdering <- sendRequest("GET", "/tables/1/columns/0/rows")
    } yield {
      assertEquals(expectedJsonAfterOrdering, testAfterOrdering.getJsonArray("rows"))
    }
  }

  @Test
  def retrieveRowsOfFirstColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.arr(
      Json.obj("id" -> 1, "values" -> Json.arr("table1row1")),
      Json.obj("id" -> 2, "values" -> Json.arr("table1row2"))
    )

    val expectedJsonAfterOrdering = Json.arr(
      Json.obj("id" -> 1, "values" -> Json.arr(Json.arr(1, "table1row1"))),
      Json.obj("id" -> 2, "values" -> Json.arr(Json.arr(2, "table1row2")))
    )

    for {
      _ <- createDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/first/rows")

      _ <- sendRequest("POST", "/tables/1/columns/2", Json.obj("ordering" -> 0, "identifier" -> true))

      testAfterOrdering <- sendRequest("GET", "/tables/1/columns/first/rows")
    } yield {
      assertEquals(expectedJson, test.getJsonArray("rows"))
      assertEquals(expectedJsonAfterOrdering, testAfterOrdering.getJsonArray("rows"))
    }
  }

  @Test
  def retrieveRowsOfFirstColumnWithConcatColumn(implicit c: TestContext): Unit = okTest {
    val expectedJsonAfterOrdering = Json.arr(
      Json.obj("id" -> 1, "values" -> Json.arr(Json.arr(1, "table1row1"))),
      Json.obj("id" -> 2, "values" -> Json.arr(Json.arr(2, "table1row2")))
    )

    for {
      _ <- createDefaultTable()
      _ <- sendRequest("GET", "/tables/1/columns/first/rows")

      _ <- sendRequest("POST", "/tables/1/columns/2", Json.obj("ordering" -> 0, "identifier" -> true))

      testAfterOrdering <- sendRequest("GET", "/tables/1/columns/first/rows")
    } yield {
      assertEquals(expectedJsonAfterOrdering, testAfterOrdering.getJsonArray("rows"))
    }
  }
}