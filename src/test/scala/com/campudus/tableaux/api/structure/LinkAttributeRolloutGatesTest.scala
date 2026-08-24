package com.campudus.tableaux.api.structure

import com.campudus.tableaux.database.model.TableauxModel.ColumnId
import com.campudus.tableaux.helper.Json
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.json.{JsonArray, JsonObject}

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

/**
  * Two link attribute features exist in the backend but are not offered yet: more than one definition per column, and a
  * multilanguage definition. Both are gated in LinkAttributeDefinition, and both gates can be lifted per test class
  * (LinkAttributeTestOverrides) - which is what makes this class necessary: it is deliberately the one that lifts
  * nothing, so it pins down what the API actually answers today.
  *
  * The count cap's rejection is covered by ChangeLinkAttributesStructureTest (which doesn't lift it either), so what is
  * left here is the multilanguage gate.
  */
@RunWith(classOf[VertxUnitRunner])
class LinkAttributeRolloutGatesTest extends TableauxTestBase {

  private def percentageAttribute(multilanguage: Boolean): JsonObject = {
    Json.obj(
      "name" -> "percentage",
      "displayName" -> Json.obj("de-DE" -> "Prozentanteil"),
      "kind" -> "integer",
      "multilanguage" -> multilanguage
    )
  }

  private def createLinkColumn(linkAttributes: JsonArray): Future[ColumnId] = {
    val columnJson = Json.obj(
      "name" -> "Test Link 1",
      "kind" -> "link",
      "toTable" -> 2,
      "linkAttributes" -> linkAttributes
    )

    for {
      _ <- createDefaultTable()
      _ <- createDefaultTable("Test Table 2", 2)
      result <- sendRequest("POST", "/tables/1/columns", Json.obj("columns" -> Json.arr(columnJson)))
    } yield result.getJsonArray("columns").getJsonObject(0).getLong("id").toLong
  }

  // Rejected while parsing, like an oversized linkAttributes array - not as an "unprocessable" langtag problem, which
  // is what a multilanguage definition on a link without langtags is (see ChangeLinkAttributesStructureTest).
  @Test
  def createLinkColumnWithMultilanguageAttributeFails(implicit c: TestContext): Unit =
    exceptionTest("error.json.linkAttributes") {
      createLinkColumn(Json.arr(percentageAttribute(multilanguage = true)))
    }

  @Test
  def changeLinkColumnToMultilanguageAttributeFails(implicit c: TestContext): Unit =
    exceptionTest("error.json.linkAttributes") {
      for {
        columnId <- createLinkColumn(Json.arr(percentageAttribute(multilanguage = false)))
        _ <- sendRequest(
          "POST",
          s"/tables/1/columns/$columnId",
          Json.obj("linkAttributes" -> Json.arr(percentageAttribute(multilanguage = true)))
        )
      } yield ()
    }

  // The gate is about `multilanguage: true` only - a language-neutral definition on a table that has langtags is
  // still the normal, supported case and must not be caught by it.
  @Test
  def createLinkColumnWithLanguageNeutralAttributeSucceeds(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(multilanguage = false)))
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertJSONEquals(Json.arr(percentageAttribute(multilanguage = false)), column.getJsonArray("linkAttributes"))
    }
  }
}
