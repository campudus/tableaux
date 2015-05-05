package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.{ Json, JsonObject }
import com.campudus.tableaux.database.SystemStructure
import com.campudus.tableaux.database.DatabaseConnection

class ErrorTest extends TableauxTestBase {

  val createTableJson = Json.obj("tableName" -> "Test Nr. 1")
  val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))
  val createNumberColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))
  val fillCellJson = Json.obj("cells" -> Json.arr(Json.obj("value" -> "Test Fill 1")))

  val errorJsonNotFound = "errors.json.not-found"
  val errorDatabaseDelete = "errors.database.delete"
  val errorDatabaseSelect = "errors.database.select"
  val errorDatabaseInsert = "errors.database.insert"
  val errorDatabaseUnknown = "errors.database.unknown"
  val errorJsonArguments = "error.json.arguments"
  val errorJsonInvalid = "error.json.invalid"
  val errorJsonNull = "error.json.null"
  val errorJsonArray = "error.json.array"
  val errorJsonObject = "error.json.object"
  val errorJsonEmpty = "error.json.empty"
  val errorJsonLink = "error.json.link"
  val notFound = "NOT FOUND"

  @Test
  def deleteNoExistingTable(): Unit = exceptionTest(notFound) {
    sendRequest("DELETE", "/tables/1")
  }

  @Test
  def deleteNoExistingColumn(): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequest("DELETE", "/tables/1/columns/1")
    } yield ()
  }

  @Test
  def deleteNoExistingRow(): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequest("DELETE", "/tables/1/rows/1")
    } yield ()
  }

  @Test
  def tryGetNoExistingTable(): Unit = exceptionTest(notFound) {
    sendRequest("GET", "/tables/1")
  }

  @Test
  def tryGetNoExistingColumn(): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequest("GET", "/tables/1/columns/1")
    } yield ()
  }

  @Test
  def tryGetNoExistingCell(): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      _ <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield ()
  }

  @Test
  def tryGetNoExistingRow(): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequest("GET", "/tables/1/rows/1")
    } yield ()
  }

  @Test
  def createTableWithNoExistingSystemTables(): Unit = exceptionTest(errorDatabaseUnknown) {
    val transaction = new DatabaseConnection(this, databaseAddress)
    val system = new SystemStructure(transaction)
    for {
      _ <- system.deinstall()
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
    } yield ()
  }

  @Test
  def createColumnWithNoExistingTable(): Unit = exceptionTest(notFound) {
    sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
  }

  @Test
  def createRowWithNoExistingTable(): Unit = exceptionTest(notFound) {
    sendRequest("POST", "/tables/1/rows")
  }

  @Test
  def fillNoExistingCell(): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", fillCellJson, "/tables/1/columns/1/rows/1")
    } yield ()
  }

  @Test
  def createTableWithoutJson(): Unit = exceptionTest(errorJsonNotFound) {
    sendRequest("POST", "/tables")
  }

  @Test
  def fillCellWithNullCells(): Unit = cellHelper(errorJsonNull, Json.obj("cells" -> null))

  @Test
  def fillCellWithEmptyJson(): Unit = cellHelper(errorJsonEmpty, Json.obj("cells" -> Json.arr()))

  @Test
  def fillCellWithInvalidJsonObject(): Unit = cellHelper(errorJsonObject, Json.obj("cells" -> Json.arr(1)))

  @Test
  def fillCellWithNullValues(): Unit = cellHelper(errorJsonNull, Json.obj("cells" -> Json.arr(Json.obj())))

  private def cellHelper(error: String, json: JsonObject): Unit = exceptionTest(error) {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      _ <- sendRequest("POST", "/tables/1/rows")
      _ <- sendRequestWithJson("POST", json, "/tables/1/columns/1/rows/1")
    } yield ()
  }

  @Test
  def createMultipleColumnsWithoutColumnsJson(): Unit = multipleColumnHelper(errorJsonNull, Json.obj("columns" -> null))

  @Test
  def createMultipleColumnsWithEmptyJson(): Unit = multipleColumnHelper(errorJsonEmpty, Json.obj("columns" -> Json.arr()))

  @Test
  def createMultipleColumnsWithoutTypeJson(): Unit = multipleColumnHelper(errorJsonNull, Json.obj("columns" -> Json.arr(Json.obj("kind" -> null, "name" -> "Test Column 1"))))

  @Test
  def createMultipleColumnsWithoutColNameJson(): Unit = multipleColumnHelper(errorJsonNull, Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> null))))

  @Test
  def createMultipleColumnsWithInvalidJsonObjects(): Unit = multipleColumnHelper(errorJsonObject, Json.obj("columns" -> Json.arr(1, "text")))

  @Test
  def createMultipleColumnsWithInvalidKindJson(): Unit = multipleColumnHelper(errorJsonInvalid, Json.obj("columns" -> Json.arr(Json.obj("kind" -> 1, "name" -> "Test Column 1"))))

  @Test
  def createMultipleColumnsWithNoStringColNameJson(): Unit = multipleColumnHelper(errorJsonInvalid, Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> 1))))

  @Test
  def createMultipleColumnsWithoutTypes(): Unit = multipleColumnHelper(errorJsonNull, Json.obj("columns" -> Json.arr(Json.obj("name" -> "Test Column 1"))))

  @Test
  def createMultipleColumnsWithMoreColNames(): Unit = multipleColumnHelper(errorJsonNull, Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text"))))

  @Test
  def createMultipleColumnsWithStartingNormalType(): Unit = multipleColumnHelper(errorJsonLink, Json.obj("columns" -> Json.arr(
    Json.obj("kind" -> "text", "name" -> "Test Column 1"),
    Json.obj("kind" -> "link", "name" -> "Test Column 2", "toTable" -> 1, "toColumn" -> 1, "fromColumn" -> 1),
    Json.obj("kind" -> "link", "name" -> "Test Column 3", "toTable" -> 1, "toColumn" -> 1, "fromColumn" -> 1))))

  @Test
  def createMultipleColumnsWithStartingLinkType(): Unit = multipleColumnHelper(errorJsonLink, Json.obj("columns" -> Json.arr(
    Json.obj("kind" -> "link", "name" -> "Test Column 1", "toTable" -> 1, "toColumn" -> 1, "fromColumn" -> 1),
    Json.obj("kind" -> "text", "name" -> "Test Column 2"),
    Json.obj("kind" -> "text", "name" -> "Test Column 3"))))

  @Test
  def createMultipleLinkColumnsWithoutToTable(): Unit = multipleColumnHelper(errorJsonNull, Json.obj("columns" -> Json.arr(
    Json.obj("kind" -> "link", "name" -> "Test Column 1", "toColumn" -> 1, "fromColumn" -> 1))))

  @Test
  def createMultipleLinkColumnsWithoutToColumn(): Unit = multipleColumnHelper(errorJsonNull, Json.obj("columns" -> Json.arr(
    Json.obj("kind" -> "link", "name" -> "Test Column 1", "toTable" -> 1, "fromColumn" -> 1))))

  @Test
  def createMultipleLinkColumnsWithoutFromColumn(): Unit = multipleColumnHelper(errorJsonNull, Json.obj("columns" -> Json.arr(
    Json.obj("kind" -> "link", "name" -> "Test Column 1", "toTable" -> 1, "toColumn" -> 1))))

  private def multipleColumnHelper(error: String, json: JsonObject): Unit = exceptionTest(error) {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", json, "/tables/1/columns")
    } yield ()
  }

  @Test
  def createMultipleFullRowsWithoutColIdJson(): Unit = multipleRowHelper(errorJsonNull,
    Json.obj("columns" -> null, "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)))))

  @Test
  def createMultipleFullRowsWithoutValueJson(): Unit = multipleRowHelper(errorJsonNull,
    Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), "rows" -> Json.arr(Json.obj("values" -> null))))

  @Test
  def createMultipleFullRowsWithEmptyColIdJson(): Unit = multipleRowHelper(errorJsonEmpty,
    Json.obj("columns" -> Json.arr(), "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)))))

  @Test
  def createMultipleFullRowsWithEmptyValueJson(): Unit = multipleRowHelper(errorJsonEmpty,
    Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), "rows" -> Json.arr(Json.obj("values" -> Json.arr()))))

  @Test
  def createMultipleFullRowsWithMoreColIds(): Unit = multipleRowHelper(errorJsonArguments,
    Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1")))))

  @Test
  def createMultipleFullRowsWithMoreValues(): Unit = multipleRowHelper(errorJsonArguments,
    Json.obj("columns" -> Json.arr(Json.obj("id" -> 1)), "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)))))

  @Test
  def createMultipleFullRowsWithInvalidColIdJson(): Unit = multipleRowHelper(errorJsonObject,
    Json.obj("columns" -> Json.arr(1, 2), "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)))))

  @Test
  def createMultipleFullRowsWithInvalidValueJson(): Unit = multipleRowHelper(errorJsonObject,
    Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), "rows" -> Json.arr("Test Field 1", 2)))

  @Test
  def createMultipleFullRowsWithEmptyColIdJsonArray(): Unit = multipleRowHelper(errorJsonEmpty,
    Json.obj("columns" -> Json.arr(), "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)))))

  @Test
  def createMultipleFullRowsWithEmptyValueJsonArray(): Unit = multipleRowHelper(errorJsonEmpty,
    Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), "rows" -> Json.arr(Json.obj("values" -> Json.arr()))))

  @Test
  def createMultipleFullRowsWithEmptyRowsJsonArray(): Unit = multipleRowHelper(errorJsonEmpty,
    Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), "rows" -> Json.arr()))

  @Test
  def createMultipleFullRowsWithStringColIdJson(): Unit = multipleRowHelper(errorJsonInvalid,
    Json.obj("columns" -> Json.arr(Json.obj("id" -> "1"), Json.obj("id" -> "2")), "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)))))

  private def multipleRowHelper(error: String, json: JsonObject): Unit = exceptionTest(error) {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      _ <- sendRequestWithJson("POST", createNumberColumnJson, "/tables/1/columns")
      _ <- sendRequestWithJson("POST", json, "/tables/1/rows")
    } yield ()
  }

  @Test
  def createCompleteTableWithNullCols(): Unit = exceptionTest(errorJsonNull) {
    val createCompleteTableJson = Json.obj(
      "tableName" -> "Test Nr. 1",
      "columns" -> null,
      "rows" -> Json.arr(
        Json.obj("values" -> Json.arr("Test Field 1", 2))))

    sendRequestWithJson("POST", createCompleteTableJson, "/tables")
  }

  @Test
  def createCompleteTableWithNullRows(): Unit = exceptionTest(errorJsonNull) {
    val createCompleteTableJson = Json.obj(
      "tableName" -> "Test Nr. 1",
      "columns" -> Json.arr(
        Json.obj("kind" -> "numeric", "name" -> "Test Column 1"),
        Json.obj("kind" -> "text", "name" -> "Test Column 2")),
      "rows" -> null)

    sendRequestWithJson("POST", createCompleteTableJson, "/tables")
  }
}