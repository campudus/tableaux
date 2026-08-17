package com.campudus.tableaux.api.content

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.helper.Json
import com.campudus.tableaux.testtools.RequestCreation.{Identifier, NumericCol, TextCol}

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.json.JsonObject

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class LinkAttributesTest extends LinkTestBase {

  private def percentageAttribute(multilanguage: Boolean = false): JsonObject = {
    Json.obj(
      "name" -> "percentage",
      "displayName" -> Json.obj("de-DE" -> "Prozentanteil"),
      "kind" -> "integer",
      "multilanguage" -> multilanguage
    )
  }

  private def postLinkColWithAttributes(
      toTableId: TableId,
      name: String = "Test Link 1",
      multilanguage: Boolean = false
  ) = {
    Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> name,
          "kind" -> "link",
          "toTable" -> toTableId,
          "linkAttributes" -> Json.arr(percentageAttribute(multilanguage))
        )
      )
    )
  }

  private def createLinkColumnWithAttributes(
      tableId: TableId,
      toTableId: TableId,
      multilanguage: Boolean = false
  ): Future[ColumnId] = {
    sendRequest(
      "POST",
      s"/tables/$tableId/columns",
      postLinkColWithAttributes(toTableId, multilanguage = multilanguage)
    )
      .map(_.getJsonArray("columns").getJsonObject(0).getLong("id").toLong)
  }

  @Test
  def createLinkWithInlineAttributes(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50))))
    )

    val expected = Json.obj(
      "status" -> "ok",
      "value" -> Json.arr(Json.obj("id" -> 1, "value" -> "table2row1", "attributes" -> Json.arr(50)))
    )

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2)
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      cell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
    } yield {
      assertEquals(expected, cell)
    }
  }

  @Test
  def createLinkWithMixedBareIdAndObjectValues(implicit c: TestContext): Unit = okTest {
    val putLinks = Json.obj(
      "value" -> Json.obj("values" -> Json.arr(1, Json.obj("id" -> 2, "attributes" -> Json.arr(75))))
    )

    val expected = Json.obj(
      "status" -> "ok",
      "value" -> Json.arr(
        Json.obj("id" -> 1, "value" -> "table2row1"),
        Json.obj("id" -> 2, "value" -> "table2row2", "attributes" -> Json.arr(75))
      )
    )

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2)
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLinks)
      cell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
    } yield {
      assertEquals(expected, cell)
    }
  }

  @Test
  def attributesSurviveLinkToTableWithConcatIdentifier(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50))))
    )

    val expected = Json.obj(
      "status" -> "ok",
      "value" -> Json.arr(Json.obj("id" -> 1, "value" -> Json.arr("target row 1", 1), "attributes" -> Json.arr(50)))
    )

    for {
      sourceTableId <- createDefaultTable()
      // target table has two identifier columns from the start, so its representing
      // column is a ConcatenateColumn - the read path then has to re-fetch each linked
      // row's value (see TableauxModel.fetchConcatValuesForLinkedRows) instead of using
      // the value the SQL projection already produced for a plain single-identifier target
      (targetTableId, _, _) <- createSimpleTableWithValues(
        "Target Table",
        List(Identifier(TextCol("name")), Identifier(NumericCol("num"))),
        List(List("target row 1", 1), List("target row 2", 2))
      )
      linkColumnId <- createLinkColumnWithAttributes(sourceTableId, targetTableId)
      _ <- sendRequest("POST", s"/tables/$sourceTableId/columns/$linkColumnId/rows/1", putLink)
      cell <- sendRequest("GET", s"/tables/$sourceTableId/columns/$linkColumnId/rows/1")
    } yield {
      assertEquals(expected, cell)
    }
  }

  @Test
  def existingRequestShapesStillWorkUnchanged(implicit c: TestContext): Unit = okTest {
    val putLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(1, 2)))

    val expected = Json.obj(
      "status" -> "ok",
      "value" -> Json.arr(
        Json.obj("id" -> 1, "value" -> "table2row1"),
        Json.obj("id" -> 2, "value" -> "table2row2")
      )
    )

    for {
      _ <- setupTwoTables()
      // column has a linkAttributes definition, but no attributes are supplied - shape/response must be unaffected
      linkColumnId <- createLinkColumnWithAttributes(1, 2)
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLinks)
      cell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
    } yield {
      assertEquals(expected, cell)
    }
  }

  @Test
  def rejectAttributesLengthMismatch(implicit c: TestContext): Unit = exceptionTest("error.json.link-attributes") {
    val putLink = Json.obj(
      "value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50, 60))))
    )

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2)
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
    } yield ()
  }

  @Test
  def rejectAttributesWrongKind(implicit c: TestContext): Unit = exceptionTest("error.json.link-attributes") {
    val putLink = Json.obj(
      "value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr("not-an-integer"))))
    )

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2)
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
    } yield ()
  }

  @Test
  def rejectAttributesOnColumnWithoutDefinition(implicit c: TestContext): Unit =
    exceptionTest("error.json.link-attributes") {
      val putLink = Json.obj(
        "value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50))))
      )

      for {
        _ <- setupTwoTables()
        linkColumnId <- createLinkColumn(1, 2, singleDirection = false)
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      } yield ()
    }

  @Test
  def multilanguageAttributeValueRoundtrips(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj(
        "values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(Json.obj("de-DE" -> 50, "en-GB" -> 60))))
      )
    )

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2, multilanguage = true)
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      cell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
    } yield {
      val attributeValue = cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getJsonObject(0)
      assertEquals(50, attributeValue.getInteger("de-DE"))
      assertEquals(60, attributeValue.getInteger("en-GB"))
    }
  }

  @Test
  def putLinkAttributesEndpointSuccess(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50))))
    )
    val putAttributes = Json.obj("attributes" -> Json.arr(75))

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2)
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1/link/1/attributes", putAttributes)
      cell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
    } yield {
      assertEquals(75, cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getInteger(0))
    }
  }

  @Test
  def putLinkAttributesEndpointLinkNotFoundFails(implicit c: TestContext): Unit =
    // any 404-status exception surfaces to tests as the literal string "NOT FOUND" (see BaseRouter's error
    // handling, which hardcodes the HTTP status message for status 404 regardless of the exception's real id)
    exceptionTest("NOT FOUND") {
      val putAttributes = Json.obj("attributes" -> Json.arr(75))

      for {
        _ <- setupTwoTables()
        linkColumnId <- createLinkColumnWithAttributes(1, 2)
        // row 1 is not linked to row 2 at all
        _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1/link/2/attributes", putAttributes)
      } yield ()
    }

  @Test
  def putLinkAttributesEndpointOnColumnWithoutDefinitionFails(implicit c: TestContext): Unit =
    exceptionTest("unprocessable.entity") {
      val putLink = Json.obj("value" -> Json.obj("to" -> 1))
      val putAttributes = Json.obj("attributes" -> Json.arr(75))

      for {
        _ <- setupTwoTables()
        linkColumnId <- createLinkColumn(1, 2, singleDirection = false)
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
        _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1/link/1/attributes", putAttributes)
      } yield ()
    }

}
