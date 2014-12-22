package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import scala.concurrent.Future
import org.vertx.scala.core.json.Json
import org.vertx.scala.core.http.HttpClient

class LinkTest extends TableauxTestBase {

  val postLinkCol = Json.obj("action" -> "createColumn", "type" -> "link", "tableId" -> 1, "columnName" -> "Test Link 1", "fromColumn" -> 1, "toTable" -> 2, "toColumn" -> 1)

  @Test
  def getLinkColumn(): Unit = okTest {
    val getColumn = Json.obj("action" -> "getColumn", "tableId" -> 1, "columnId" -> 3)
    val expectedJson = Json.obj("columnId" -> 3, "columnName" -> "Test Link 1", "type" -> "link")
    
    for {
      (c, tables) <- setupTables()
      _ <- sendRequest("POST", c, postLinkCol, "/tables/1/columns")
      test <- sendRequest("GET", c, getColumn, "/tables/1/columns/3")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createLinkColumn(): Unit = okTest {
    
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 3, "columnType" -> "link", "toTable" -> 2, "toColumn" -> 1)
    
    for {
      (c, tables) <- setupTables()
      test <- sendRequest("POST", c, postLinkCol, "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def fillSingleLinkCell(): Unit = okTest {
    val fillLinkCellJson = Json.obj("action" -> "fillCell", "type" -> "link", "tableId" -> 1, "columnId" -> 3, "rowId" -> 1, "value" -> Json.arr(1,1))
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 3, "rowId" -> 1, "value" -> Json.arr(Json.obj("id" -> 1,"value" -> "Test Fill 1")))
    
    for {
      (c, tables) <- setupTables()
      columnId <- sendRequest("POST", c, postLinkCol, "/tables/1/columns") map {_.getLong("columnId")}
      test <- sendRequest("POST", c, fillLinkCellJson, s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  private def setupTables(): Future[(HttpClient, Seq[Long])] = {
    val c = createClient()
    
    for {
      id1 <- setupDefaultTable(c)
      id2 <- setupDefaultTable(c, "Test Table 2")
    } yield (c, List(id1, id2))
  }

  private def setupDefaultTable(c: HttpClient, name: String = "Test Table 1"): Future[Long] = {
    val postTable = Json.obj("action" -> "createTable", "tableName" -> name)
    val postTextCol = Json.obj("action" -> "createColumn", "type" -> "text", "columnName" -> "Test Column 1")
    val postNumCol = Json.obj("action" -> "createColumn", "type" -> "numeric", "columnName" -> "Test Column 2")
    val createRowJson = Json.obj("action" -> "createRow")
    val fillStringCellJson = Json.obj("action" -> "fillCell", "type" -> "text", "columnId" -> 1, "rowId" -> 1, "value" -> "Test Fill 1")
    val fillStringCellJson2 = Json.obj("action" -> "fillCell", "type" -> "text", "columnId" -> 1, "rowId" -> 2, "value" -> "Test Fill 2")
    val fillNumberCellJson = Json.obj("action" -> "fillCell", "type" -> "numeric", "columnId" -> 2, "rowId" -> 1, "value" -> 1)
    val fillNumberCellJson2 = Json.obj("action" -> "fillCell", "type" -> "numeric", "columnId" -> 2, "rowId" -> 2, "value" -> 2)
    
    for {
      tableId <- sendRequest("POST", c, postTable, "/tables") map {js => js.getLong("tableId")}
      _ <- sendRequest("POST", c, postTextCol, s"/tables/$tableId/columns")
      _ <- sendRequest("POST", c, postNumCol, s"/tables/$tableId/columns")
      _ <- sendRequest("POST", c, createRowJson, s"/tables/$tableId/rows")
      _ <- sendRequest("POST", c, createRowJson, s"/tables/$tableId/rows")
      _ <- sendRequest("POST", c, fillStringCellJson, s"/tables/$tableId/columns/1/rows/1")
      _ <- sendRequest("POST", c, fillStringCellJson2, s"/tables/$tableId/columns/1/rows/2")
      _ <- sendRequest("POST", c, fillNumberCellJson, s"/tables/$tableId/columns/2/rows/1")
      _ <- sendRequest("POST", c, fillNumberCellJson2, s"/tables/$tableId/columns/2/rows/2")
    } yield tableId
  }
}