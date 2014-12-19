package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import scala.concurrent.Future
import org.vertx.scala.core.json.Json
import org.vertx.scala.core.http.HttpClient

class LinkTest extends TableauxTestBase {

  val getTable = Json.obj("action" -> "getTable", "tableId" -> 1)
  
  @Test
  def getLinkColumn(): Unit = {
    fail("not implemented")
  }

//  @Test
//  def createLinkColumn(): Unit = okTest {
//    val postLinkCol = Json.obj("action" -> "createColumn", "type" -> "link", "tableId" -> 1, "columnName" -> "Test Link 1")
//    
//    val expectedJson = Json.obj("tableId" -> 1, "tableName" -> "Test Nr. 1", "cols" -> Json.arr(), "rows" -> Json.arr())
//    val expectedJson2 = Json.obj("tableId" -> 1, "tableName" -> "Test Nr. 1", "cols" -> Json.arr(), "rows" -> Json.arr())
//    
//    for {
//      c <- setupTables()
//      _ <- sendRequest("POST", c, postLinkCol, "/tables/1/columns")
//      test <- sendRequest("GET", c, getTable, "/tables/1")
//      test2 <- sendRequest("GET", c, getTable, "/tables/2")
//    } yield {
//      assertEquals(expectedJson, test)
//      assertEquals(expectedJson2, test2)
//    }
//  }

  @Test
  def fillSingleLinkCell(): Unit = {
    fail("not implemented")
  }

  private def setupTables(): Future[HttpClient] = {
    val c = createClient()
    
    for {
      _ <- setupDefaultTable(c)
      _ <- setupDefaultTable(c)
    } yield c
  }
  
  private def setupDefaultTable(c: HttpClient): Future[Unit] = {
    val postTable = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
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
    } yield ()
  }
}