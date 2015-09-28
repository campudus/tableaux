package com.campudus.tableaux

import org.junit.Test
import org.vertx.scala.core.json.Json
import org.vertx.testtools.VertxAssert._

class GetTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Table 1")
  val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))
  val createNumberColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))

  @Test
  def retrieveEmptyTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
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
  def retrieveWithColumnTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false),
        Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false)),
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
      test <- sendRequest("GET", "/completetable/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveWithColumnAndRowTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false),
        Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false)),
      "page" -> Json.obj(
        "offset" -> null,
        "limit" -> null,
        "totalSize" -> 1
      ),
      "rows" -> Json.arr(
        Json.obj("id" -> 1, "values" -> Json.arr(null, null))))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
      _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("GET", "/completetable/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveFullTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false),
        Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false)),
      "page" -> Json.obj(
        "offset" -> null,
        "limit" -> null,
        "totalSize" -> 2
      ),
      "rows" -> Json.arr(
        Json.obj("id" -> 1, "values" -> Json.arr("table1row1", 1)),
        Json.obj("id" -> 2, "values" -> Json.arr("table1row2", 2))))

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/completetable/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1"
    )

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveAllTables(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "tables" -> Json.arr(
      Json.obj("id" -> 1, "name" -> "Test Table 1"),
      Json.obj("id" -> 2, "name" -> "Test Table 2")
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
  def retrieveColumns(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(
      Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false),
      Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false)
    ))

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveStringColumn(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false)

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveNumberColumn(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false)

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveRow(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 1, "values" -> Json.arr("table1row1", 1))

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/rows/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveRows(): Unit = okTest {
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
  def retrievePagedRows(): Unit = okTest {
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
  def retrieveCell(): Unit = okTest {
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
  def retrieveRowsOfSpecificColumn(): Unit = okTest {
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