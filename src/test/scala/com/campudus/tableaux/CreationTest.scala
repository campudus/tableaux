package com.campudus.tableaux

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class CreationTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")

  val createTextColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))
  val createShortTextColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "shorttext", "name" -> "Test Column 1")))
  val createRichTextColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "richtext", "name" -> "Test Column 1")))

  val createNumberColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))
  val createBooleanColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "boolean", "name" -> "Test Column 3")))

  @Test
  def createTable(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 1)
    val expectedJson2 = Json.obj("status" -> "ok", "id" -> 2)

    for {
      test1 <- sendRequest("POST", "/tables", createTableJson)
      test2 <- sendRequest("POST", "/tables", createTableJson)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createTextColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1)))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2)))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createTextColumnJson)
      test2 <- sendRequest("POST", "/tables/1/columns", createTextColumnJson)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createShortTextColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1)))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2)))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createShortTextColumnJson)
      test2 <- sendRequest("POST", "/tables/1/columns", createShortTextColumnJson)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createRichTextColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1)))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2)))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createRichTextColumnJson)
      test2 <- sendRequest("POST", "/tables/1/columns", createRichTextColumnJson)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createNumberColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1)))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2)))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
      test2 <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createBooleanColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "ordering" -> 1)))
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "ordering" -> 2)))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
      test2 <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createMultipleColumns(implicit c: TestContext): Unit = okTest {
    val jsonObj = Json.obj("columns" -> Json.arr(
      Json.obj("kind" -> "numeric", "name" -> "Test Column 1"),
      Json.obj("kind" -> "text", "name" -> "Test Column 2")))

    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(
      Json.obj("id" -> 1, "ordering" -> 1),
      Json.obj("id" -> 2, "ordering" -> 2)))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test <- sendRequest("POST", "/tables/1/columns", jsonObj)
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createMultipleColumnsWithOrdering(implicit c: TestContext): Unit = okTest {
    val jsonObj = Json.obj("columns" -> Json.arr(
      Json.obj("kind" -> "numeric", "name" -> "Test Column 1", "ordering" -> 2),
      Json.obj("kind" -> "text", "name" -> "Test Column 2", "ordering" -> 1)))
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(
      Json.obj("id" -> 1, "ordering" -> 2),
      Json.obj("id" -> 2, "ordering" -> 1)))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test <- sendRequest("POST", "/tables/1/columns", jsonObj)
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createRow(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "id" -> 1)
    val expectedJson2 = Json.obj("status" -> "ok", "id" -> 2)

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test1 <- sendRequest("POST", "/tables/1/rows")
      test2 <- sendRequest("POST", "/tables/1/rows")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createFullRow(implicit c: TestContext): Unit = okTest {
    val valuesRow = Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2))))
    val expectedJson = Json.obj("status" -> "ok", "rows" -> Json.arr(Json.obj("id" -> 1)))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createTextColumnJson)
      _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
      test <- sendRequest("POST", "/tables/1/rows", valuesRow)
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createMultipleFullRows(implicit c: TestContext): Unit = okTest {
    val valuesRow = Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
      "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)), Json.obj("values" -> Json.arr("Test Field 2", 5))))
    val expectedJson = Json.obj("status" -> "ok", "rows" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createTextColumnJson)
      _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
      test <- sendRequest("POST", "/tables/1/rows", valuesRow)
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createCompleteTable(implicit c: TestContext): Unit = okTest {
    val createCompleteTableJson = Json.obj(
      "name" -> "Test Nr. 1",
      "columns" -> Json.arr(
        Json.obj("kind" -> "text", "name" -> "Test Column 1"),
        Json.obj("kind" -> "numeric", "name" -> "Test Column 2")),
      "rows" -> Json.arr(
        Json.obj("values" -> Json.arr("Test Field 1", 1)),
        Json.obj("values" -> Json.arr("Test Field 2", 2))))

    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "ordering" -> 1),
        Json.obj("id" -> 2, "ordering" -> 2)),
      "rows" -> Json.arr(
        Json.obj("id" -> 1),
        Json.obj("id" -> 2)))

    for {
      test <- sendRequest("POST", "/completetable", createCompleteTableJson)
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createCompleteTableWithOrdering(implicit c: TestContext): Unit = okTest {
    val createCompleteTableJson = Json.obj(
      "name" -> "Test Nr. 1",
      "columns" -> Json.arr(
        Json.obj("kind" -> "text", "name" -> "Test Column 1", "ordering" -> 2),
        Json.obj("kind" -> "numeric", "name" -> "Test Column 2", "ordering" -> 1)),
      "rows" -> Json.arr(
        Json.obj("values" -> Json.arr("Test Field 1", 1)),
        Json.obj("values" -> Json.arr("Test Field 2", 2))))

    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "columns" -> Json.arr(
        Json.obj("id" -> 2, "ordering" -> 1),
        Json.obj("id" -> 1, "ordering" -> 2)),
      "rows" -> Json.arr(
        Json.obj("id" -> 1),
        Json.obj("id" -> 2)))

    for {
      test <- sendRequest("POST", "/completetable", createCompleteTableJson)
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createCompleteTableWithoutRows(implicit c: TestContext): Unit = okTest {
    val createCompleteTableJson = Json.obj(
      "name" -> "Test Nr. 1",
      "columns" -> Json.arr(
        Json.obj("kind" -> "text", "name" -> "Test Column 1"),
        Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))

    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "ordering" -> 1),
        Json.obj("id" -> 2, "ordering" -> 2)),
      "rows" -> Json.arr())

    for {
      test <- sendRequest("POST", "/completetable", createCompleteTableJson)
    } yield {
      assertEquals(expectedJson, test)
    }
  }
}