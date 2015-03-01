package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.{ Json, JsonObject }
import com.campudus.tableaux.database.SystemStructure
import com.campudus.tableaux.database.DatabaseConnection

class ErrorTest extends TableauxTestBase {

  val createTableJson = Json.obj("tableName" -> "Test Nr. 1")
  val createStringColumnJson = Json.obj("type" -> Json.arr("text"), "columnName" -> Json.arr("Test Column 1"))
  val createNumberColumnJson = Json.obj("type" -> Json.arr("numeric"), "columnName" -> Json.arr("Test Column 2"))
  val fillCellJson = Json.obj("type" -> "text", "value" -> "Test Fill 1")

  val errorJsonNotFound = "errors.json.not-found"
  val errorDatabaseDelete = "errors.database.delete"
  val errorDatabaseSelect = "errors.database.select"
  val errorDatabaseInsert = "errors.database.insert"
  val errorDatabaseUnknown = "errors.database.unknown"
  val errorJsonArguments = "error.json.arguments"
  val errorJsonInvalid = "error.json.invalid"
  val errorJsonNull = "error.json.null"
  val errorJsonArray = "error.json.array"
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
  def getNoExistingTable(): Unit = exceptionTest(notFound) {
    sendRequest("GET", "/tables/1")
  }

  @Test
  def getNoExistingColumn(): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequest("GET", "/tables/1/columns/1")
    } yield ()
  }

  @Test
  def getNoExistingCell(): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      _ <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield ()
  }

  @Test
  def getNoExistingRow(): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequest("GET", "/tables/1/rows/1")
    } yield ()
  }

  @Test
  def createTableWithNoExistingSystemTables(): Unit = exceptionTest(errorDatabaseUnknown) {
    val transaction = new DatabaseConnection(this)
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
  def createMultipleColumnsWithoutTypeJson(): Unit = multipleColumnHelper(errorJsonNull, Json.obj("type" -> null, "columnName" -> Json.arr("Test Column 1", "Test Column 2")))

  @Test
  def createMultipleColumnsWithoutColNameJson(): Unit = multipleColumnHelper(errorJsonNull, Json.obj("type" -> Json.arr("numeric", "text"), "columnName" -> null))

  @Test
  def createMultipleColumnsWithInvalidTypeJson(): Unit = multipleColumnHelper(errorJsonArray, Json.obj("type" -> 1, "columnName" -> Json.arr("Test Column 1", "Test Column 2")))

  @Test
  def createMultipleColumnsWithInvalidColNameJson(): Unit = multipleColumnHelper(errorJsonArray, Json.obj("type" -> Json.arr("numeric", "text"), "columnName" -> 1))

  @Test
  def createMultipleColumnsWithNoStringColNameJson(): Unit = multipleColumnHelper(errorJsonInvalid, Json.obj("type" -> Json.arr("numeric", "text"), "columnName" -> Json.arr(1, "Test Column 2")))

  @Test
  def createMultipleColumnsWithEmptyTypeJson(): Unit = multipleColumnHelper(errorJsonEmpty, Json.obj("type" -> Json.arr(), "columnName" -> Json.arr("Test Column 1", "Test Column 2")))

  @Test
  def createMultipleColumnsWithEmptyColNameJson(): Unit = multipleColumnHelper(errorJsonEmpty, Json.obj("type" -> Json.arr("numeric", "text"), "columnName" -> Json.arr()))

  @Test
  def createMultipleColumnsWithMoreTypes(): Unit = multipleColumnHelper(errorJsonArguments, Json.obj("type" -> Json.arr("numeric", "text"), "columnName" -> Json.arr("Test Column 1")))

  @Test
  def createMultipleColumnsWithMoreColNames(): Unit = multipleColumnHelper(errorJsonArguments, Json.obj("type" -> Json.arr("numeric"), "columnName" -> Json.arr("Test Column 1", "Test Column 2")))

  @Test
  def createMultipleColumnsWithStartingNormalType(): Unit = multipleColumnHelper(errorJsonLink, Json.obj("type" -> Json.arr("numeric", "link", "link"), "columnName" -> Json.arr("Test Column 1", "Test Column 2", "Test Column 3")))

  @Test
  def createMultipleColumnsWithStartingLinkType(): Unit = multipleColumnHelper(errorJsonLink, Json.obj("type" -> Json.arr("link", "text", "text"), "columnName" -> Json.arr("Test Column 1", "Test Column 2", "Test Column 3")))

  private def multipleColumnHelper(error: String, json: JsonObject): Unit = exceptionTest(error) {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", json, "/tables/1/columns")
    } yield ()
  }

  @Test
  def createMultipleFullRowsWithoutColIdJson(): Unit = multipleRowHelper(errorJsonNull, Json.obj("columnIds" -> null, "values" -> Json.arr(Json.arr("Test Field 1", 2))))

  @Test
  def createMultipleFullRowsWithoutValueJson(): Unit = multipleRowHelper(errorJsonNull, Json.obj("columnIds" -> Json.arr(Json.arr(1, 2)), "values" -> null))

  @Test
  def createMultipleFullRowsWithEmptyColIdJson(): Unit = multipleRowHelper(errorJsonEmpty, Json.obj("columnIds" -> Json.arr(), "values" -> Json.arr(Json.arr("Test Field 1", 2))))

  @Test
  def createMultipleFullRowsWithEmptyValueJson(): Unit = multipleRowHelper(errorJsonEmpty, Json.obj("columnIds" -> Json.arr(Json.arr(1, 2)), "values" -> Json.arr()))

  @Test
  def createMultipleFullRowsWithMoreColIds(): Unit = multipleRowHelper(errorJsonArguments, Json.obj("columnIds" -> Json.arr(Json.arr(1, 2)), "values" -> Json.arr(Json.arr("Test Field 1"))))

  @Test
  def createMultipleFullRowsWithMoreValues(): Unit = multipleRowHelper(errorJsonArguments, Json.obj("columnIds" -> Json.arr(Json.arr(1)), "values" -> Json.arr(Json.arr("Test Field 1", 2))))

  @Test
  def createMultipleFullRowsWithInvalidColIdJson(): Unit = multipleRowHelper(errorJsonArray, Json.obj("columnIds" -> Json.arr(1), "values" -> Json.arr(Json.arr("Test Field 1", 2))))

  @Test
  def createMultipleFullRowsWithInvalidValueJson(): Unit = multipleRowHelper(errorJsonArray, Json.obj("columnIds" -> Json.arr(Json.arr(1, 2)), "values" -> Json.arr("Test Field 1")))

  @Test
  def createMultipleFullRowsWithEmptyColIdJsonArray(): Unit = multipleRowHelper(errorJsonEmpty, Json.obj("columnIds" -> Json.arr(Json.arr()), "values" -> Json.arr(Json.arr("Test Field 1", 2))))

  @Test
  def createMultipleFullRowsWithEmptyValueJsonArray(): Unit = multipleRowHelper(errorJsonEmpty, Json.obj("columnIds" -> Json.arr(Json.arr(1, 2)), "values" -> Json.arr(Json.arr())))

  @Test
  def createMultipleFullRowsWithStringColIdJson(): Unit = multipleRowHelper(errorJsonInvalid, Json.obj("columnIds" -> Json.arr(Json.arr("1", 2)), "values" -> Json.arr(Json.arr("Test Field 1", 2))))

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
      "cols" -> null,
      "rows" -> Json.obj(
        "columnIds" -> Json.arr(Json.arr(1, 2), Json.arr(1, 2)),
        "values" -> Json.arr(Json.arr("Test Fill 1", 1), Json.arr("Test Fill 2", 2))))

    sendRequestWithJson("POST", createCompleteTableJson, "/tables")
  }

  @Test
  def createCompleteTableWithNullRows(): Unit = exceptionTest(errorJsonNull) {
    val createCompleteTableJson = Json.obj(
      "tableName" -> "Test Nr. 1",
      "cols" -> Json.obj(
        "type" -> Json.arr("text", "numeric"),
        "columnName" -> Json.arr("Test Column 1", "Test Column 2")),
      "rows" -> null)

    sendRequestWithJson("POST", createCompleteTableJson, "/tables")
  }
}