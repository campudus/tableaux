package com.campudus.tableaux

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
      "rows" -> Json.arr())

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
        Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false, "identifier" -> false),
        Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false, "identifier" -> false),
        Json.obj("id" -> 3, "name" -> "Test Column 3", "kind" -> "boolean", "ordering" -> 3, "multilanguage" -> false, "identifier" -> false)),
      "page" -> Json.obj(
        "offset" -> null,
        "limit" -> null,
        "totalSize" -> 0
      ),
      "rows" -> Json.arr())

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
        Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false, "identifier" -> false),
        Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false, "identifier" -> false),
        Json.obj("id" -> 3, "name" -> "Test Column 3", "kind" -> "boolean", "ordering" -> 3, "multilanguage" -> false, "identifier" -> false)),
      "page" -> Json.obj(
        "offset" -> null,
        "limit" -> null,
        "totalSize" -> 1
      ),
      "rows" -> Json.arr(
        Json.obj("id" -> 1, "values" -> Json.arr(null, null, null))))

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
      Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false, "identifier" -> false),
      Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false, "identifier" -> false),
      Json.obj("id" -> 3, "name" -> "Test Column 3", "kind" -> "boolean", "ordering" -> 3, "multilanguage" -> false, "identifier" -> false)
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
      "rows" -> rows
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
  def retrieveTable(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "hidden" -> false
    )

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveAllTables(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "tables" -> Json.arr(
      Json.obj("id" -> 1, "name" -> "Test Table 1", "hidden" -> false),
      Json.obj("id" -> 2, "name" -> "Test Table 2", "hidden" -> false)
    ))

    for {
      _ <- setupDefaultTable("Test Table 1")
      _ <- setupDefaultTable("Test Table 2")
      test <- sendRequest("GET", "/tables")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveColumns(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(
      Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false, "identifier" -> false),
      Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false, "identifier" -> false)
    ))

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveStringColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false, "identifier" -> false)

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveNumberColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false, "identifier" -> false)

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveRow(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 1, "values" -> Json.arr("table1row1", 1))

    for {
      _ <- setupDefaultTable()
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
      _ <- setupDefaultTable()
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
      _ <- setupDefaultTable()
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
      _ <- setupDefaultTable()
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
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/2/rows")
    } yield {
      assertEquals(expectedJson, test)
    }
  }
}