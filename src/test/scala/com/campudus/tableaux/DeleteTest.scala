package com.campudus.tableaux

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

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
      // Create a second column because we can't delete the only and last column of a table
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

    val failed = Json.obj("failed" -> "failed")

    for {
      table1 <- sendRequest("POST", "/tables", createTableJson).map(_.getLong("id"))
      table2 <- sendRequest("POST", "/tables", createTableJson).map(_.getLong("id"))

      _ <- sendRequest("POST", s"/tables/$table1/columns", createIdentifierStringColumnJson)
      _ <- sendRequest("POST", s"/tables/$table2/columns", createIdentifierStringColumnJson)

      _ <- sendRequest("POST", s"/tables/$table1/columns", createLinkColumnJson)

      deleteResult <- sendRequest("DELETE", "/tables/1")

      table2Result <- sendRequest("GET", "/tables/2/columns")

      table1Result <- sendRequest("GET", "/tables/1").recoverWith({ case _ => Future.successful(failed) })
    } yield {
      assertEquals(expectedOkJson, deleteResult)
      // Check if back-link from Table 2 is Table 1 was deleted
      assertEquals(1, table2Result.getJsonArray("columns").size())
      // Table 1 is already gone
      assertEquals(failed, table1Result)
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

      deleteResult <- sendRequest("DELETE", "/tables/1/columns/2")

      table1Columns <- sendRequest("GET", "/tables/1/columns")
      table2Columns <- sendRequest("GET", "/tables/2/columns")
    } yield {
      assertEquals(expectedOkJson, deleteResult)

      assertEquals(1, table1Columns.getJsonArray("columns").size())
      assertEquals(2, table2Columns.getJsonArray("columns").size())

      val backlink = Json.obj(
        "kind" -> "link",
        "toTable" -> 1
      )

      assertContains(backlink, table2Columns.getJsonArray("columns").getJsonObject(1))
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
      // Create a second column because we can't delete the only and last column of a table
      _ <- sendRequest("POST", s"/tables/$table1/columns", createStringColumnJson)

      test <- sendRequest("DELETE", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedOkJson, test)
    }
  }
}