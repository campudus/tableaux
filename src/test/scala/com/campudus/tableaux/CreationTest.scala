package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.Json

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
class CreationTest extends TableauxTestBase {

  val createTableJson = Json.obj("tableName" -> "Test Nr. 1")
  val createStringColumnJson = Json.obj("type" -> Json.arr("text"), "columnName" -> Json.arr("Test Column 1"))
  val createNumberColumnJson = Json.obj("type" -> Json.arr("numeric"), "columnName" -> Json.arr("Test Column 2"))

  @Test
  def createTable(): Unit = okTest {
    val expectedJson = Json.obj("tableId" -> 1, "tableName" -> "Test Nr. 1")
    val expectedJson2 = Json.obj("tableId" -> 2, "tableName" -> "Test Nr. 1")

    for {
      test1 <- sendRequestWithJson("POST", createTableJson, "/tables")
      test2 <- sendRequestWithJson("POST", createTableJson, "/tables")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createStringColumn(): Unit = okTest {
    val expectedJson = Json.obj("tableId" -> 1, "cols" -> Json.arr(Json.obj("columnId" -> 1, "columnName" -> "Test Column 1", "type" -> "text")))
    val expectedJson2 = Json.obj("tableId" -> 1, "cols" -> Json.arr(Json.obj("columnId" -> 2, "columnName" -> "Test Column 1", "type" -> "text")))

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      test1 <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      test2 <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createNumberColumn(): Unit = okTest {
    val expectedJson = Json.obj("tableId" -> 1, "cols" -> Json.arr(Json.obj("columnId" -> 1, "columnName" -> "Test Column 2", "type" -> "numeric")))
    val expectedJson2 = Json.obj("tableId" -> 1, "cols" -> Json.arr(Json.obj("columnId" -> 2, "columnName" -> "Test Column 2", "type" -> "numeric")))

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      test1 <- sendRequestWithJson("POST", createNumberColumnJson, "/tables/1/columns")
      test2 <- sendRequestWithJson("POST", createNumberColumnJson, "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createMultipleColumns(): Unit = okTest {
    val jsonObj = Json.obj("type" -> Json.arr("numeric", "text"), "columnName" -> Json.arr("Test Column 1", "Test Column 2"))
    val expectedJson = Json.obj("tableId" -> 1, "cols" -> Json.arr(
      Json.obj("columnId" -> 1, "columnName" -> "Test Column 1", "type" -> "numeric"),
      Json.obj("columnId" -> 2, "columnName" -> "Test Column 2", "type" -> "text")))

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      test <- sendRequestWithJson("POST", jsonObj, "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createRow(): Unit = okTest {
    val expectedJson = Json.obj("tableId" -> 1, "rowId" -> 1)
    val expectedJson2 = Json.obj("tableId" -> 1, "rowId" -> 2)

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      test1 <- sendRequest("POST", "/tables/1/rows")
      test2 <- sendRequest("POST", "/tables/1/rows")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createFullRow(): Unit = okTest {
    val valuesRow = Json.obj("columnIds" -> Json.arr(Json.arr(1, 2)), "values" -> Json.arr(Json.arr("Test Field 1", 2)))
    val expectedJson1 = Json.obj("tableId" -> 1, "rows" -> Json.arr(Json.obj("rowId" -> 1, "values" -> Json.arr("Test Field 1", 2))))

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      _ <- sendRequestWithJson("POST", createNumberColumnJson, "/tables/1/columns")
      test <- sendRequestWithJson("POST", valuesRow, "/tables/1/rows")
    } yield {
      assertEquals(expectedJson1, test)
    }
  }

  @Test
  def createMultipleFullRows(): Unit = okTest {
    val valuesRow = Json.obj("columnIds" -> Json.arr(Json.arr(1, 2), Json.arr(1, 2)), "values" -> Json.arr(Json.arr("Test Field 1", 2), Json.arr("Test Field 2", 5)))
    val expectedJson1 = Json.obj("tableId" -> 1, "rows" -> Json.arr(
      Json.obj("rowId" -> 1, "values" -> Json.arr("Test Field 1", 2)),
      Json.obj("rowId" -> 2, "values" -> Json.arr("Test Field 2", 5))))

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      _ <- sendRequestWithJson("POST", createNumberColumnJson, "/tables/1/columns")
      test <- sendRequestWithJson("POST", valuesRow, "/tables/1/rows")
    } yield {
      assertEquals(expectedJson1, test)
    }
  }
}
