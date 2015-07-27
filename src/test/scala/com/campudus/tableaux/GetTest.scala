package com.campudus.tableaux

import org.junit.Test
import org.vertx.scala.core.json.Json
import org.vertx.testtools.VertxAssert._
import scala.concurrent.Future
import org.vertx.scala.core.http.HttpClient

class GetTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Table 1")
  val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))
  val createNumberColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))

  @Test
  def getEmptyTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "columns" -> Json.arr(),
      "rows" -> Json.arr())

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      test <- sendRequest("GET", "/completetable/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getWithColumnTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false),
        Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false)),
      "rows" -> Json.arr())

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      _ <- sendRequestWithJson("POST", createNumberColumnJson, "/tables/1/columns")
      test <- sendRequest("GET", "/completetable/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getWithColumnAndRowTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false),
        Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false)),
      "rows" -> Json.arr(
        Json.obj("id" -> 1, "values" -> Json.arr(null, null))))

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      _ <- sendRequestWithJson("POST", createNumberColumnJson, "/tables/1/columns")
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("GET", "/completetable/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getFullTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Test Table 1",
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false),
        Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false)),
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
  def getTable(): Unit = okTest {
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
  def getAllTables(): Unit = okTest {
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
  def getColumns(): Unit = okTest {
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
  def getStringColumn(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1, "multilanguage" -> false)

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getNumberColumn(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2, "multilanguage" -> false)

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getRow(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 1, "values" -> Json.arr("table1row1", 1))

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/rows/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getRows(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "rows" -> Json.arr(
      Json.obj("id" -> 1, "values" -> Json.arr("table1row1", 1)),
      Json.obj("id" -> 2, "values" -> Json.arr("table1row2", 2))
    ))

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/rows")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getCell(): Unit = okTest {
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
}