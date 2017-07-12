package com.campudus.tableaux.api

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.SystemModel
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonObject}

@RunWith(classOf[VertxUnitRunner])
class ErrorTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")
  val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))

  val createNumberColumnJson =
    Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))
  val fillCellJson = Json.obj("value" -> "Test Fill 1")

  val errorJsonNotFound = "error.json.notfound"
  val errorDatabaseDelete = "error.database.delete"
  val errorDatabaseSelect = "error.database.select"
  val errorDatabaseInsert = "error.database.insert"
  val errorDatabaseUnknown = "error.database.unknown"
  val errorJsonArguments = "error.json.arguments"
  val errorJsonInvalid = "error.json.invalid"
  val errorJsonNull = "error.json.null"
  val errorJsonArray = "error.json.array"
  val errorJsonObject = "error.json.object"
  val errorJsonEmpty = "error.json.empty"
  val errorJsonLink = "error.json.link"
  val notFound = "NOT FOUND"

  @Test
  def sendInvalidJsonToValidRoute(implicit c: TestContext): Unit = exceptionTest(errorJsonInvalid) {
    sendRequest("POST", "/tables", "{ 'invalid': true', funny' }{")
  }

  @Test
  def requestWrongRoute(implicit c: TestContext): Unit = exceptionTest(notFound) {
    sendRequest("GET", "/wrong/route")
  }

  @Test
  def deleteNoExistingTable(implicit c: TestContext): Unit = exceptionTest(notFound) {
    sendRequest("DELETE", "/tables/1")
  }

  @Test
  def deleteLastColumn(implicit c: TestContext): Unit = exceptionTest("error.database.delete-last-column") {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
      _ <- sendRequest("DELETE", "/tables/1/columns/1")
    } yield ()
  }

  @Test
  def deleteNonExistingColumn(implicit c: TestContext): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
      _ <- sendRequest("DELETE", "/tables/1/columns/2")
    } yield ()
  }

  @Test
  def deleteColumnOfTableWithoutColumns(implicit c: TestContext): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("DELETE", "/tables/1/columns/1")
    } yield ()
  }

  @Test
  def deleteNoExistingRow(implicit c: TestContext): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("DELETE", "/tables/1/rows/1")
    } yield ()
  }

  @Test
  def retrieveNoExistingTable(implicit c: TestContext): Unit = exceptionTest(notFound) {
    sendRequest("GET", "/tables/1")
  }

  @Test
  def retrieveNoExistingColumn(implicit c: TestContext): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("GET", "/tables/1/columns/1")
    } yield ()
  }

  @Test
  def retrieveNoExistingCell(implicit c: TestContext): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
      _ <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield ()
  }

  @Test
  def retrieveNoExistingRow(implicit c: TestContext): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("GET", "/tables/1/rows/1")
    } yield ()
  }

  @Test
  def createTableWithNoExistingSystemTables(implicit c: TestContext): Unit = exceptionTest(errorDatabaseUnknown) {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val system = SystemModel(dbConnection)

    for {
      _ <- system.uninstall()
      _ <- sendRequest("POST", "/tables", createTableJson)
    } yield ()
  }

  @Test
  def createColumnWithNoExistingTable(implicit c: TestContext): Unit = exceptionTest(notFound) {
    sendRequest("POST", "/tables/1/columns", createStringColumnJson)
  }

  @Test
  def createRowWithNoExistingTable(implicit c: TestContext): Unit = exceptionTest(notFound) {
    sendRequest("POST", "/tables/1/rows")
  }

  @Test
  def fillNoExistingCell(implicit c: TestContext): Unit = exceptionTest(notFound) {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillCellJson)
    } yield ()
  }

  @Test
  def createTableWithoutJson(implicit c: TestContext): Unit = exceptionTest(errorJsonNotFound) {
    sendRequest("POST", "/tables")
  }

  @Test
  def createMultipleColumnsWithoutColumnsJson(implicit c: TestContext): Unit = {
    multipleColumnHelper(errorJsonNull, Json.obj("columns" -> null))
  }

  @Test
  def createMultipleColumnsWithEmptyJson(implicit c: TestContext): Unit = {
    multipleColumnHelper(errorJsonEmpty, Json.obj("columns" -> Json.arr()))
  }

  @Test
  def createMultipleColumnsWithoutTypeJson(implicit c: TestContext): Unit = {
    multipleColumnHelper(errorJsonNull,
                         Json.obj("columns" -> Json.arr(Json.obj("kind" -> null, "name" -> "Test Column 1"))))
  }

  @Test
  def createMultipleColumnsWithoutColNameJson(implicit c: TestContext): Unit = {
    multipleColumnHelper(errorJsonNull, Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> null))))
  }

  @Test
  def createMultipleColumnsWithInvalidJsonObjects(implicit c: TestContext): Unit = {
    multipleColumnHelper(errorJsonObject, Json.obj("columns" -> Json.arr(1, "text")))
  }

  @Test
  def createMultipleColumnsWithInvalidKindJson(implicit c: TestContext): Unit = {
    multipleColumnHelper(errorJsonInvalid,
                         Json.obj("columns" -> Json.arr(Json.obj("kind" -> 1, "name" -> "Test Column 1"))))
  }

  @Test
  def createMultipleColumnsWithNoStringColNameJson(implicit c: TestContext): Unit = {
    multipleColumnHelper(errorJsonInvalid, Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> 1))))
  }

  @Test
  def createMultipleColumnsWithoutTypes(implicit c: TestContext): Unit = {
    multipleColumnHelper(errorJsonNull, Json.obj("columns" -> Json.arr(Json.obj("name" -> "Test Column 1"))))
  }

  @Test
  def createMultipleColumnsWithMoreColNames(implicit c: TestContext): Unit = {
    multipleColumnHelper(errorJsonNull, Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text"))))
  }

  @Test
  def createMultipleLinkColumnsWithoutToTable(implicit c: TestContext): Unit = {
    multipleColumnHelper(errorJsonNull,
                         Json.obj("columns" -> Json.arr(Json.obj("kind" -> "link", "name" -> "Test Column 1"))))
  }

  private def multipleColumnHelper(error: String, json: JsonObject)(implicit c: TestContext): Unit = {
    exceptionTest(error) {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", json)
      } yield ()
    }
  }

  @Test
  def createMultipleFullRowsWithoutColIdJson(implicit c: TestContext): Unit = {
    multipleRowHelper(
      errorJsonNull,
      Json.obj("columns" -> null, "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)))))
  }

  @Test
  def createMultipleFullRowsWithoutValueJson(implicit c: TestContext): Unit = {
    multipleRowHelper(errorJsonNull,
                      Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
                               "rows" -> Json.arr(Json.obj("values" -> null))))
  }

  @Test
  def createMultipleFullRowsWithEmptyColIdJson(implicit c: TestContext): Unit = {
    multipleRowHelper(
      errorJsonEmpty,
      Json.obj("columns" -> Json.arr(), "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)))))
  }

  @Test
  def createMultipleFullRowsWithEmptyValueJson(implicit c: TestContext): Unit = {
    multipleRowHelper(errorJsonEmpty,
                      Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
                               "rows" -> Json.arr(Json.obj("values" -> Json.arr()))))
  }

  @Test
  def createMultipleFullRowsWithMoreColIds(implicit c: TestContext): Unit = {
    multipleRowHelper(errorJsonArguments,
                      Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
                               "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1")))))
  }

  @Test
  def createMultipleFullRowsWithMoreValues(implicit c: TestContext): Unit = {
    multipleRowHelper(errorJsonArguments,
                      Json.obj("columns" -> Json.arr(Json.obj("id" -> 1)),
                               "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)))))
  }

  @Test
  def createMultipleFullRowsWithInvalidColIdJson(implicit c: TestContext): Unit = {
    multipleRowHelper(
      errorJsonObject,
      Json.obj("columns" -> Json.arr(1, 2), "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)))))
  }

  @Test
  def createMultipleFullRowsWithInvalidValueJson(implicit c: TestContext): Unit = {
    multipleRowHelper(
      errorJsonObject,
      Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), "rows" -> Json.arr("Test Field 1", 2)))
  }

  @Test
  def createMultipleFullRowsWithEmptyColIdJsonArray(implicit c: TestContext): Unit = {
    multipleRowHelper(
      errorJsonEmpty,
      Json.obj("columns" -> Json.arr(), "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2)))))
  }

  @Test
  def createMultipleFullRowsWithEmptyValueJsonArray(implicit c: TestContext): Unit = {
    multipleRowHelper(errorJsonEmpty,
                      Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
                               "rows" -> Json.arr(Json.obj("values" -> Json.arr()))))
  }

  @Test
  def createMultipleFullRowsWithEmptyRowsJsonArray(implicit c: TestContext): Unit = {
    multipleRowHelper(errorJsonEmpty,
                      Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), "rows" -> Json.arr()))
  }

  @Test
  def createMultipleFullRowsWithStringColIdJson(implicit c: TestContext): Unit = {
    multipleRowHelper(
      errorJsonInvalid,
      Json.obj("columns" -> Json.arr(Json.obj("id" -> "1"), Json.obj("id" -> "2")),
               "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2))))
    )
  }

  private def multipleRowHelper(error: String, json: JsonObject)(implicit c: TestContext): Unit = {
    exceptionTest(error) {
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
        _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
        _ <- sendRequest("POST", "/tables/1/rows", json)
      } yield ()
    }
  }

  @Test
  def createCompleteTableWithNullCols(implicit c: TestContext): Unit = {
    exceptionTest(errorJsonNull) {
      val createCompleteTableJson = Json.obj("name" -> "Test Nr. 1",
                                             "columns" -> null,
                                             "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 2))))

      sendRequest("POST", "/completetable", createCompleteTableJson)
    }
  }

  @Test
  def createCompleteTableWithNullRows(implicit c: TestContext): Unit = {
    exceptionTest(errorJsonNull) {
      val createCompleteTableJson = Json.obj(
        "name" -> "Test Nr. 1",
        "columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 1"),
                              Json.obj("kind" -> "text", "name" -> "Test Column 2")),
        "rows" -> null
      )

      sendRequest("POST", "/completetable", createCompleteTableJson)
    }
  }
}
