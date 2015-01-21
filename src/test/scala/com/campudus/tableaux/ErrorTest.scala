package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.Json
import com.campudus.tableaux.database.SystemStructure
import com.campudus.tableaux.database.DatabaseConnection

class ErrorTest extends TableauxTestBase {

  val createTableJson = Json.obj("tableName" -> "Test Nr. 1")
  val createStringColumnJson = Json.obj("type" -> "text", "columnName" -> "Test Column 1")
  val fillCellJson = Json.obj("type" -> "text", "value" -> "Test Fill 1")

  @Test
  def deleteNoExistingTable(): Unit = exceptionTest {
    sendRequest("DELETE", "/tables/1")
  }

  @Test
  def deleteNoExistingColumn(): Unit = exceptionTest {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequest("DELETE", "/tables/1/columns/1")
    } yield ()
  }

  @Test
  def deleteNoExistingRow(): Unit = exceptionTest {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequest("DELETE", "/tables/1/rows/1")
    } yield ()
  }

  @Test
  def getNoExistingTable(): Unit = exceptionTest {
    sendRequest("GET", "/tables/1")
  }

  @Test
  def getNoExistingColumn(): Unit = exceptionTest {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequest("GET", "/tables/1/columns/1")
    } yield ()
  }

  @Test
  def getNoExistingCell(): Unit = exceptionTest {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      _ <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield ()
  }

  @Test
  def getNoExistingRow(): Unit = exceptionTest {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequest("GET", "/tables/1/rows/1")
    } yield ()
  }

  @Test
  def createTableWithNoExistingSystemTables(): Unit = exceptionTest {
    val transaction = new DatabaseConnection(this)
    val system = new SystemStructure(transaction)
    for {
      _ <- system.deinstall()
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
    } yield ()
  }

  @Test
  def createColumnWithNoExistingTable(): Unit = exceptionTest {
    sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
  }

  @Test
  def createRowWithNoExistingTable(): Unit = exceptionTest {
    sendRequest("POST", "/tables/1/rows")
  }

  @Test
  def fillNoExistingCell(): Unit = exceptionTest {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", fillCellJson, "/tables/1/columns/1/rows/1")
    } yield ()
  }

  @Test
  def createTableWithoutJson(): Unit = exceptionTest {
    sendRequest("POST", "/tables")
  }
}