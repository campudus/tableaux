package com.campudus.tableaux

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class DeleteTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")
  val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))
  val createIdentifierStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1", "identifier" -> true)))

  val expectedOkJson = Json.obj("status" -> "ok")

  @Test
  def deleteEmptyTable(implicit c: TestContext): Unit = okTest {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test <- sendRequest("DELETE", "/tables/1")
    } yield {
      c.assertEquals(expectedOkJson, test)
    }
  }

  @Test
  def deleteTableWithColumn(implicit c: TestContext): Unit = okTest {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
      test <- sendRequest("DELETE", "/tables/1")
    } yield {
      assertEquals(expectedOkJson, test)
    }
  }

  @Test
  def deleteColumn(implicit c: TestContext): Unit = okTest {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
      test <- sendRequest("DELETE", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedOkJson, test)
    }
  }

  @Test
  def deleteRow(implicit c: TestContext): Unit = okTest {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("DELETE", "/tables/1/rows/1")
    } yield {
      assertEquals(expectedOkJson, test)
    }
  }

  @Test
  def deleteTableWithLink(implicit c: TestContext): Unit = okTest {
    val createLinkColumnJson = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "toTable" -> 2
        )
      )
    )

    for {
      table1 <- sendRequest("POST", "/tables", createTableJson).map(_.getLong("id"))
      table2 <- sendRequest("POST", "/tables", createTableJson).map(_.getLong("id"))

      _ <- sendRequest("POST", s"/tables/$table1/columns", createIdentifierStringColumnJson)
      _ <- sendRequest("POST", s"/tables/$table2/columns", createIdentifierStringColumnJson)

      _ <- sendRequest("POST", s"/tables/$table1/columns", createLinkColumnJson)

      test <- sendRequest("DELETE", "/tables/1")
    } yield {
      assertEquals(expectedOkJson, test)
      // TODO check 404 at GET /tables/1 and check link is gone in /tables/2
    }
  }

  @Test
  def deleteLinkColumn(implicit c: TestContext): Unit = okTest {
    val createLinkColumnJson = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "toTable" -> 2
        )
      )
    )

    for {
      table1 <- sendRequest("POST", "/tables", createTableJson).map(_.getLong("id"))
      table2 <- sendRequest("POST", "/tables", createTableJson).map(_.getLong("id"))

      _ <- sendRequest("POST", s"/tables/$table1/columns", createIdentifierStringColumnJson)
      _ <- sendRequest("POST", s"/tables/$table2/columns", createIdentifierStringColumnJson)

      _ <- sendRequest("POST", s"/tables/$table1/columns", createLinkColumnJson)

      test <- sendRequest("DELETE", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedOkJson, test)
      // TODO check GET /tables/1/columns/2 for 404 and GET /tables/2/columns/2 is still there (bidirectional link)
    }
  }

  @Test
  def deleteAttachmentColumn(implicit c: TestContext): Unit = okTest {
    val createAttachmentColumnJson = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Attachment 1",
          "kind" -> "attachment"
        )
      )
    )

    for {
      table1 <- sendRequest("POST", "/tables", createTableJson).map(_.getLong("id"))

      _ <- sendRequest("POST", s"/tables/$table1/columns", createAttachmentColumnJson)

      test <- sendRequest("DELETE", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedOkJson, test)
    }
  }
}