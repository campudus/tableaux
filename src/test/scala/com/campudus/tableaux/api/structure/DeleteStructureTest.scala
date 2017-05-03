package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.{RequestCreation, TableauxTestBase}
import org.vertx.scala.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

import scala.concurrent.Future
import scala.util.Random

@RunWith(classOf[VertxUnitRunner])
class DeleteStructureTest extends TableauxTestBase {

  def createTableJson: JsonObject = {
    val random = Random.nextInt()
    Json.obj("name" -> s"Test Nr. $random")
  }

  val createStringColumnJson = RequestCreation.Columns().add(RequestCreation.TextCol("Test Column 1")).getJson
  val createIdentifierStringColumnJson =
    RequestCreation.Columns().add(RequestCreation.Identifier(RequestCreation.TextCol("Test Column 1"))).getJson

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
  def deleteColumn(implicit c: TestContext): Unit = {
    okTest{
      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        shouldBeZeroColumns <- sendRequest("GET", "/tables/1/columns")

        _ <- sendRequest("POST",
          "/tables/1/columns",
          RequestCreation.Columns().add(RequestCreation.TextCol("Test Column 1")).getJson)
        shouldBeOneColumns <- sendRequest("GET", "/tables/1/columns")

        // Create a second column because we can't delete the only and last column of a table
        _ <- sendRequest("POST",
          "/tables/1/columns",
          RequestCreation.Columns().add(RequestCreation.TextCol("Test Column 2")).getJson)
        shouldBeTwoColumns <- sendRequest("GET", "/tables/1/columns")

        test <- sendRequest("DELETE", "/tables/1/columns/1")
        afterDelete <- sendRequest("GET", "/tables/1/columns")
      } yield {
        assertEquals(expectedOkJson, test)

        assertEquals(0, shouldBeZeroColumns.getJsonArray("columns", Json.emptyArr()).size())
        assertEquals(1, shouldBeOneColumns.getJsonArray("columns", Json.emptyArr()).size())
        assertEquals(2, shouldBeTwoColumns.getJsonArray("columns", Json.emptyArr()).size())
        assertEquals(1, afterDelete.getJsonArray("columns", Json.emptyArr()).size())
      }
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
  def deleteLinkColumn(implicit c: TestContext): Unit = {
    okTest{
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
  }

  @Test
  def deleteLinkInBothDirectionsAndCheckForDependentRows(implicit c: TestContext): Unit = {
    okTest{
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

        _ <- sendRequest(
          "POST",
          s"/tables/$table2/rows",
          Json.obj(
            "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
            "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test", Json.emptyArr())))
          )
        )

        _ <- sendRequest(
          "POST",
          s"/tables/$table1/rows",
          Json.obj(
            "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
            "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test", Json.arr(1))))
          )
        )

        _ <- sendRequest("DELETE", "/tables/1/columns/2")
        _ <- sendRequest("DELETE", "/tables/2/columns/2")

        dependentRows <- sendRequest("GET", s"/tables/$table2/rows/1/dependent")

        table1Columns <- sendRequest("GET", "/tables/1/columns")
        table2Columns <- sendRequest("GET", "/tables/2/columns")
      } yield {
        assertTrue(dependentRows.getJsonArray("dependentRows").isEmpty)

        assertEquals(1, table1Columns.getJsonArray("columns").size())
        assertEquals(1, table2Columns.getJsonArray("columns").size())
      }
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
