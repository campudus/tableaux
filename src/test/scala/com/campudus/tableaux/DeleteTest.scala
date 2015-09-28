package com.campudus.tableaux

import org.junit.Test
import org.vertx.scala.core.json.Json
import org.vertx.testtools.VertxAssert._

class DeleteTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")
  val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))

  val expectedOkJson = Json.obj("status" -> "ok")

  @Test
  def deleteEmptyTable(): Unit = okTest {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      test <- sendRequest("DELETE", "/tables/1")
    } yield {
      assertEquals(expectedOkJson, test)
    }
  }

  @Test
  def deleteTableWithColumn(): Unit = okTest {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
      test <- sendRequest("DELETE", "/tables/1")
    } yield {
      assertEquals(expectedOkJson, test)
    }
  }

  @Test
  def deleteColumn(): Unit = okTest {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
      test <- sendRequest("DELETE", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedOkJson, test)
    }
  }

  @Test
  def deleteRow(): Unit = okTest {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("DELETE", "/tables/1/rows/1")
    } yield {
      assertEquals(expectedOkJson, test)
    }
  }

  @Test
  def deleteTableWithLink(): Unit = okTest {
    val createLinkColumnJson = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "fromColumn" -> 1,
          "toTable" -> 2,
          "toColumn" -> 1
        )
      )
    )

    for {
      table1 <- sendRequest("POST", "/tables", createTableJson).map(_.getLong("id"))
      table2 <- sendRequest("POST", "/tables", createTableJson).map(_.getLong("id"))

      _ <- sendRequest("POST", s"/tables/$table1/columns", createStringColumnJson)
      _ <- sendRequest("POST", s"/tables/$table2/columns", createStringColumnJson)

      _ <- sendRequest("POST", s"/tables/$table1/columns", createLinkColumnJson)

      test <- sendRequest("DELETE", "/tables/1")
    } yield {
      assertEquals(expectedOkJson, test)
    }
  }

  @Test
  def deleteLinkColumn(): Unit = okTest {
    val createLinkColumnJson = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "fromColumn" -> 1,
          "toTable" -> 2,
          "toColumn" -> 1
        )
      )
    )

    for {
      table1 <- sendRequest("POST", "/tables", createTableJson).map(_.getLong("id"))
      table2 <- sendRequest("POST", "/tables", createTableJson).map(_.getLong("id"))

      _ <- sendRequest("POST", s"/tables/$table1/columns", createStringColumnJson)
      _ <- sendRequest("POST", s"/tables/$table2/columns", createStringColumnJson)

      _ <- sendRequest("POST", s"/tables/$table1/columns", createLinkColumnJson)

      test <- sendRequest("DELETE", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedOkJson, test)
    }
  }

  @Test
  def deleteAttachmentColumn(): Unit = okTest {
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