package com.campudus.tableaux.api.structure

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.helper.Json
import com.campudus.tableaux.testtools.{LinkAttributeTestOverrides, TableauxTestBase}

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.json.{JsonArray, JsonObject}

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

/**
  * Structure-level tests for a link column's `linkAttributes` definition: creating it, changing it, and migrating
  * values that were already stored under the previous definition.
  *
  * Multilanguage definitions are gated off for rollout, so this class lifts that gate - the reshape and collapse
  * migrations it covers are the whole reason the gate can be lifted later at all. That the gate holds by default is
  * covered by LinkAttributeRolloutGatesTest; the one-definition cap is not lifted here and still applies.
  */
@RunWith(classOf[VertxUnitRunner])
class ChangeLinkAttributesStructureTest extends TableauxTestBase with LinkAttributeTestOverrides {

  override protected def testMultilanguageLinkAttributesSupported: Boolean = true

  private def percentageAttribute(
      kind: String = "integer",
      multilanguage: Boolean = false,
      name: String = "percentage"
  ): JsonObject = {
    Json.obj(
      "name" -> name,
      "displayName" -> Json.obj("de-DE" -> "Prozentanteil"),
      "kind" -> kind,
      "multilanguage" -> multilanguage
    )
  }

  private def createLinkColumn(
      linkAttributes: JsonArray = Json.arr(),
      formatPattern: Option[String] = None
  ): Future[ColumnId] = {
    val baseJson =
      Json.obj(
        "name" -> "Test Link 1",
        "kind" -> "link",
        "toTable" -> 2,
        // explicit rather than relying on the default - a backlink column actually being
        // created is load-bearing for the backlink-focused tests further down
        "singleDirection" -> false,
        "linkAttributes" -> linkAttributes
      )
    val columnJson = formatPattern match {
      case Some(pattern) => baseJson.mergeIn(Json.obj("formatPattern" -> pattern))
      case None => baseJson
    }
    val postJson = Json.obj("columns" -> Json.arr(columnJson))

    for {
      _ <- createDefaultTable()
      _ <- createDefaultTable("Test Table 2", 2)
      result <- sendRequest("POST", "/tables/1/columns", postJson)
    } yield result.getJsonArray("columns").getJsonObject(0).getLong("id").toLong
  }

  // createLinkColumn() is bidirectional by default, so table 2 gets an auto-created backlink
  // column pointing back at table 1 - this finds its id so tests can read from that side.
  private def findBacklinkColumnId(toTable: TableId): Future[ColumnId] = {
    sendRequest("GET", "/tables/2/columns").map(
      _.getJsonArray("columns")
        .asScala
        .map(_.asInstanceOf[JsonObject])
        .collectFirst({
          case col if col.getString("kind") == "link" && col.getLong("toTable") == toTable => col.getLong("id").toLong
        })
        .get
    )
  }

  @Test
  def createLinkColumnWithLinkAttributes(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute()))
      result <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertJSONEquals(Json.arr(percentageAttribute()), result.getJsonArray("linkAttributes"))
    }
  }

  @Test
  def createLinkColumnWithTooManyLinkAttributesFails(implicit c: TestContext): Unit =
    exceptionTest("error.json.linkAttributes") {
      createLinkColumn(Json.arr(percentageAttribute(), percentageAttribute()))
    }

  @Test
  def createLinkColumnWithDisallowedLinkAttributeKindFails(implicit c: TestContext): Unit =
    exceptionTest("error.json.linkAttributes") {
      createLinkColumn(Json.arr(percentageAttribute(kind = "link")))
    }

  // the name is the only handle a value has - {{attributes.<name>}} in a formatPattern, matched by a \w-based
  // regex - so a name that regex can never produce is rejected instead of being stored unreferenceable
  private def createLinkColumnWithNameFails(name: String)(implicit c: TestContext): Unit =
    exceptionTest("error.json.linkAttributes") {
      createLinkColumn(Json.arr(percentageAttribute(name = name)))
    }

  @Test
  def createLinkColumnWithEmptyLinkAttributeNameFails(implicit c: TestContext): Unit =
    createLinkColumnWithNameFails("")

  @Test
  def createLinkColumnWithBlankLinkAttributeNameFails(implicit c: TestContext): Unit =
    createLinkColumnWithNameFails("   ")

  // a dot would make a token like {{attributes.a.b}} ambiguous
  @Test
  def createLinkColumnWithDottedLinkAttributeNameFails(implicit c: TestContext): Unit =
    createLinkColumnWithNameFails("percentage.value")

  @Test
  def createLinkColumnWithSpaceInLinkAttributeNameFails(implicit c: TestContext): Unit =
    createLinkColumnWithNameFails("percent age")

  @Test
  def createLinkColumnWithLinkAttributeNameOfLettersDigitsUnderscoreSucceeds(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(name = "percentage_2")))
      result <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertEquals("percentage_2", result.getJsonArray("linkAttributes").getJsonObject(0).getString("name"))
    }
  }

  // the change path parses linkAttributes with the same parser, so it rejects the same names
  @Test
  def changeLinkColumnToInvalidLinkAttributeNameFails(implicit c: TestContext): Unit =
    exceptionTest("error.json.linkAttributes") {
      for {
        columnId <- createLinkColumn(Json.arr(percentageAttribute()))
        _ <- sendRequest(
          "POST",
          s"/tables/1/columns/$columnId",
          Json.obj("linkAttributes" -> Json.arr(percentageAttribute(name = "percent age")))
        )
      } yield ()
    }

  @Test
  def changeLinkColumnAddLinkAttributes(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn()
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute()))
      )
      result <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertJSONEquals(Json.arr(percentageAttribute()), result.getJsonArray("linkAttributes"))
    }
  }

  @Test
  def changeLinkColumnToTooManyLinkAttributesFails(implicit c: TestContext): Unit =
    exceptionTest("error.json.linkAttributes") {
      for {
        columnId <- createLinkColumn(Json.arr(percentageAttribute()))
        _ <- sendRequest(
          "POST",
          s"/tables/1/columns/$columnId",
          Json.obj("linkAttributes" -> Json.arr(percentageAttribute(), percentageAttribute()))
        )
      } yield ()
    }

  @Test
  def changeLinkColumnLinkAttributesForbiddenForNonLinkColumns(implicit c: TestContext): Unit =
    exceptionTest("error.request.forbidden.column") {
      for {
        _ <- createDefaultTable()
        _ <- sendRequest(
          "POST",
          "/tables/1/columns/1",
          Json.obj("linkAttributes" -> Json.arr(percentageAttribute()))
        )
      } yield ()
    }

  @Test
  def changeLinkColumnWithExplicitEmptyLinkAttributesClearsValues(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50)))))

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute()))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId", Json.obj("linkAttributes" -> Json.arr()))
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertFalse(column.containsKey("linkAttributes"))
      assertFalse(cell.getJsonArray("value").getJsonObject(0).containsKey("attributes"))
    }
  }

  @Test
  def changeLinkColumnOmittingLinkAttributesLeavesItUntouched(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute()))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId", Json.obj("name" -> "renamed"))
      result <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertJSONEquals(Json.arr(percentageAttribute()), result.getJsonArray("linkAttributes"))
    }
  }

  @Test
  def changeLinkColumnRenamingLinkAttributePreservesValues(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50)))))

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(name = "percentage")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      // rename only - same kind, same multilanguage flag, just a different name/displayName label
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(name = "percent")))
      )
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals("percent", column.getJsonArray("linkAttributes").getJsonObject(0).getString("name"))
      assertEquals(50, cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getInteger(0))
    }
  }

  @Test
  def changeLinkColumnLinkAttributesKindMigrationSucceeds(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50)))))

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(kind = "integer")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(kind = "numeric")))
      )
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals("numeric", column.getJsonArray("linkAttributes").getJsonObject(0).getString("kind"))
      val migratedValue = cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getValue(0)
      assertEquals(50.0, migratedValue.asInstanceOf[Number].doubleValue(), 0.001)
    }
  }

  @Test
  def changeLinkColumnLinkAttributesKindMigrationFailsAndRollsBack(implicit c: TestContext): Unit = okTest {
    val putLink =
      Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr("abc")))))
    val failed = Json.obj("failed" -> "failed")

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(kind = "text")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      changeResult <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(kind = "integer")))
      ).recoverWith({ case _ => Future.successful(failed) })
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals(failed, changeResult)
      assertEquals("text", column.getJsonArray("linkAttributes").getJsonObject(0).getString("kind"))
      assertEquals("abc", cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getString(0))
    }
  }

  @Test
  def changeLinkColumnMultilanguageFalseToTrueDuplicatesValue(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50)))))

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(multilanguage = false)))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(multilanguage = true)))
      )
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      val attributeValue = cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getJsonObject(0)
      assertEquals(50, attributeValue.getInteger("de-DE"))
      assertEquals(50, attributeValue.getInteger("en-GB"))
    }
  }

  @Test
  def changeLinkColumnMultilanguageTrueToFalseCollapsesValue(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj(
        "values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(Json.obj("de-DE" -> 50, "en-GB" -> 75))))
      )
    )

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(multilanguage = true)))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(multilanguage = false)))
      )
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals(50, cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getInteger(0))
    }
  }

  // a stored null is a value that is already empty - duplicating it under every langtag would only respell that,
  // so the reshape leaves the slot alone instead of producing an object full of nulls
  @Test
  def changeLinkColumnMultilanguageFalseToTrueLeavesNullValueUntouched(implicit c: TestContext): Unit = okTest {
    val putLink =
      Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr().addNull()))))

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(multilanguage = false)))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(multilanguage = true)))
      )
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals(Json.arr().addNull(), cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes"))
    }
  }

  // collapsing has to pick the first langtag that carries an actual value: a cleared langtag comes back from `->` as
  // a JSON null rather than a SQL NULL, so a plain COALESCE would stop at it and discard the value behind it
  @Test
  def changeLinkColumnMultilanguageTrueToFalseSkipsClearedLangtags(implicit c: TestContext): Unit = okTest {
    // de-DE comes first in the table's langtags and is explicitly cleared, so en-GB's value has to win
    val putLink = Json.obj(
      "value" -> Json.obj(
        "values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(Json.obj("de-DE" -> null, "en-GB" -> 75))))
      )
    )

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(multilanguage = true)))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(multilanguage = false)))
      )
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals(75, cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getInteger(0))
    }
  }

  @Test
  def changeLinkColumnLinkAttributesKindMigrationKeepsClearedLangtags(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj(
        "values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(Json.obj("de-DE" -> null, "en-GB" -> 50))))
      )
    )

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(kind = "integer", multilanguage = true)))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(kind = "numeric", multilanguage = true)))
      )
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      val attributeValue = cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getJsonObject(0)
      // the cleared langtag keeps its key instead of being dropped by the cast
      assertEquals(Set("de-DE", "en-GB"), attributeValue.fieldNames().asScala.toSet)
      assertNull(attributeValue.getValue("de-DE"))
      assertEquals(50.0, attributeValue.getValue("en-GB").asInstanceOf[Number].doubleValue(), 0.001)
    }
  }

  // degenerate but reachable: a multilanguage value with every langtag removed. Aggregating over its zero entries
  // yields a SQL NULL, which strict jsonb_set would turn into a wiped attributes column instead of an empty object.
  @Test
  def changeLinkColumnMultilanguageLinkAttributesKindMigrationKeepsEmptyValue(implicit c: TestContext): Unit = okTest {
    val putLink =
      Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(Json.obj())))))

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(kind = "integer", multilanguage = true)))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(kind = "numeric", multilanguage = true)))
      )
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals(Json.arr(Json.obj()), cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes"))
    }
  }

  // a slot holding null has nothing to cast, so the kind change goes through and the value stays put - casting it
  // would hand strict jsonb_set a SQL NULL, which wipes the whole attributes column
  @Test
  def changeLinkColumnLinkAttributesKindMigrationLeavesNullValueUntouched(implicit c: TestContext): Unit =
    kindMigrationOnNullValueKeepsSlot(multilanguage = false)

  // same for a multilanguage slot, where deconstructing the null would additionally error out ("cannot call
  // jsonb_each_text on a non-object") and roll back the whole column change
  @Test
  def changeLinkColumnMultilanguageLinkAttributesKindMigrationLeavesNullValueUntouched(
      implicit c: TestContext
  ): Unit = kindMigrationOnNullValueKeepsSlot(multilanguage = true)

  private def kindMigrationOnNullValueKeepsSlot(multilanguage: Boolean)(implicit c: TestContext): Unit = okTest {
    val putLink =
      Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr().addNull()))))

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(kind = "integer", multilanguage = multilanguage)))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(kind = "numeric", multilanguage = multilanguage)))
      )
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals("numeric", column.getJsonArray("linkAttributes").getJsonObject(0).getString("kind"))
      assertEquals(Json.arr().addNull(), cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes"))
    }
  }

  @Test
  def changeLinkColumnFormatPatternAccepted(implicit c: TestContext): Unit = okTest {
    val pattern = "{{value}} ({{attributes.percentage}}%)"

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute()))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId", Json.obj("formatPattern" -> pattern))
      result <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertEquals(pattern, result.getString("formatPattern"))
    }
  }

  @Test
  def changeLinkColumnFormatPatternRejectedForUnknownToken(implicit c: TestContext): Unit =
    exceptionTest("unprocessable.entity") {
      for {
        columnId <- createLinkColumn(Json.arr(percentageAttribute()))
        _ <- sendRequest(
          "POST",
          s"/tables/1/columns/$columnId",
          Json.obj("formatPattern" -> "{{attributes.doesNotExist}}")
        )
      } yield ()
    }

  @Test
  def createLinkColumnFormatPatternAccepted(implicit c: TestContext): Unit = okTest {
    val pattern = "{{value}} ({{attributes.percentage}}%)"

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute()), formatPattern = Some(pattern))
      result <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertEquals(pattern, result.getString("formatPattern"))
    }
  }

  @Test
  def createLinkColumnFormatPatternRejectedForUnknownToken(implicit c: TestContext): Unit =
    // same pattern that changeLinkColumnFormatPatternRejectedForUnknownToken rejects on the change path -
    // creating a link column must be rejected the same way instead of silently storing a broken pattern
    exceptionTest("unprocessable.entity") {
      createLinkColumn(Json.arr(percentageAttribute()), formatPattern = Some("{{attributes.doesNotExist}}"))
    }

  // linkAttributes definitions live once in system_link_table, keyed by link_id, shared by both
  // sides of a bidirectional link (see ColumnModel.retrieveLinkInformation). Attribute values
  // likewise live once per edge in link_table_<linkId>.attributes. So on a fresh read (nothing
  // cached yet), both the definition and the value are symmetric - identical whether read from
  // the forward link column or from its auto-created backlink.
  @Test
  def backlinkColumnSharesLinkAttributesDefinitionAndValue(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(30)))))

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(kind = "integer")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      backlinkColumnId <- findBacklinkColumnId(toTable = 1)

      forwardColumn <- sendRequest("GET", s"/tables/1/columns/$columnId")
      backlinkColumn <- sendRequest("GET", s"/tables/2/columns/$backlinkColumnId")

      forwardCell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
      backlinkCell <- sendRequest("GET", s"/tables/2/columns/$backlinkColumnId/rows/1")
    } yield {
      // same definition on both sides
      assertJSONEquals(forwardColumn.getJsonArray("linkAttributes"), backlinkColumn.getJsonArray("linkAttributes"))

      // same attribute value on both sides - it's one edge, read from either end
      assertEquals(30, forwardCell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getInteger(0))
      assertEquals(30, backlinkCell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getInteger(0))
    }
  }

  // Regression test for a fixed cache-invalidation gap: StructureController.changeColumn used to
  // invalidate only the cell-value cache of the column that was actually changed
  // (eventClient.invalidateColumn at the end of changeColumn), unlike the row-write path
  // (TableauxModel.invalidateCellAndDependentColumns), which also walks retrieveDependencies.
  // Without that walk, a backlink cell already cached before a linkAttributes kind change kept
  // serving the pre-migration value even though the column's own definition (and the forward
  // side's cell) were already up to date. invalidateDependentColumnCaches closes that gap.
  @Test
  def backlinkCellCacheIsInvalidatedAfterLinkAttributesKindChange(implicit c: TestContext): Unit = okTest {
    val putLink =
      Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr("50")))))

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(kind = "text")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      backlinkColumnId <- findBacklinkColumnId(toTable = 1)

      // populate the backlink cell's cache before the definition changes
      backlinkCellBefore <- sendRequest("GET", s"/tables/2/columns/$backlinkColumnId/rows/1")

      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(kind = "integer")))
      )

      forwardCellAfter <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
      backlinkCellAfter <- sendRequest("GET", s"/tables/2/columns/$backlinkColumnId/rows/1")
    } yield {
      val beforeValue = backlinkCellBefore.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getValue(0)
      assertEquals("50", beforeValue)

      // forward side is freshly read (never cached before the change) - correctly migrated to a number
      val forwardValue = forwardCellAfter.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getValue(0)
      assertEquals(50, forwardValue)

      // backlink side was cached before the change too, but the cache is now invalidated as part
      // of the change - re-read fresh and reflects the same migrated number
      val backlinkValue =
        backlinkCellAfter.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getValue(0)
      assertEquals(50, backlinkValue)
    }
  }

  // Same invalidateDependentColumnCaches fix, but exercised via a GroupColumn instead of a link -
  // no linkAttributes involved. A GroupColumn's cell value is a plain array of its grouped
  // columns' own values, so casting one grouped column's kind must invalidate the GroupColumn's
  // already-cached cell too (the retrieveDependentGroupColumn half of the fix, as opposed to the
  // retrieveDependencies/backlink half exercised above).
  @Test
  def changeColumnKindInvalidatesDependentGroupColumnCellCache(implicit c: TestContext): Unit = okTest {
    val createTextColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Text")))
    val createNumericColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Number")))

    for {
      tableId <- sendRequest("POST", "/tables", Json.obj("name" -> "Group Cache Test")).map(_.getLong("id"))

      textColumnId <- sendRequest("POST", s"/tables/$tableId/columns", createTextColumnJson)
        .map(_.getJsonArray("columns").getJsonObject(0).getLong("id").toLong)
      numericColumnId <- sendRequest("POST", s"/tables/$tableId/columns", createNumericColumnJson)
        .map(_.getJsonArray("columns").getJsonObject(0).getLong("id").toLong)

      createGroupColumnJson = Json.obj(
        "columns" -> Json.arr(
          Json.obj("kind" -> "group", "name" -> "Group", "groups" -> Json.arr(textColumnId, numericColumnId))
        )
      )
      groupColumnId <- sendRequest("POST", s"/tables/$tableId/columns", createGroupColumnJson)
        .map(_.getJsonArray("columns").getJsonObject(0).getLong("id").toLong)

      _ <- sendRequest(
        "POST",
        s"/tables/$tableId/rows",
        Json.obj(
          "columns" -> Json.arr(Json.obj("id" -> textColumnId), Json.obj("id" -> numericColumnId)),
          "rows" -> Json.arr(Json.obj("values" -> Json.arr("5", 10)))
        )
      )

      // populate the group column's cell cache before the structure change
      groupCellBefore <- sendRequest("GET", s"/tables/$tableId/columns/$groupColumnId/rows/1")

      // cast the text column's stored "5" to the number 5
      _ <- sendRequest("POST", s"/tables/$tableId/columns/$textColumnId", Json.obj("kind" -> "numeric"))

      textCellAfter <- sendRequest("GET", s"/tables/$tableId/columns/$textColumnId/rows/1")
      groupCellAfter <- sendRequest("GET", s"/tables/$tableId/columns/$groupColumnId/rows/1")
    } yield {
      assertEquals(Json.arr("5", 10), groupCellBefore.getJsonArray("value"))

      // the changed column itself is freshly read (never cached before the change) - correctly cast
      assertEquals(5, textCellAfter.getInteger("value"))

      // the group column depends on it and was cached before the change - now invalidated, reflects
      // the same cast number instead of the stale pre-migration string
      assertEquals(Json.arr(5, 10), groupCellAfter.getJsonArray("value"))
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // formatPattern and linkAttributes constrain each other, so changing either one alone has to be validated against
  // the other as it currently stands - otherwise a rename leaves a pattern pointing at a token that no longer exists.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  def changeLinkAttributesRenameLeavingFormatPatternDanglingFails(implicit c: TestContext): Unit =
    exceptionTest("unprocessable.entity") {
      for {
        columnId <- createLinkColumn(
          Json.arr(percentageAttribute()),
          formatPattern = Some("{{value}} ({{attributes.percentage}}%)")
        )
        // no formatPattern in this request - the stored one still references {{attributes.percentage}}
        _ <- sendRequest(
          "POST",
          s"/tables/1/columns/$columnId",
          Json.obj("linkAttributes" -> Json.arr(percentageAttribute(name = "share")))
        )
      } yield ()
    }

  @Test
  def clearingLinkAttributesLeavingFormatPatternDanglingFails(implicit c: TestContext): Unit =
    exceptionTest("unprocessable.entity") {
      for {
        columnId <- createLinkColumn(
          Json.arr(percentageAttribute()),
          formatPattern = Some("{{attributes.percentage}}")
        )
        _ <- sendRequest("POST", s"/tables/1/columns/$columnId", Json.obj("linkAttributes" -> Json.arr()))
      } yield ()
    }

  // Renaming and re-pointing the pattern in one request is the supported way through: the check sees the pair as it
  // will be after the change, not the mix of old pattern and new definitions.
  @Test
  def changeLinkAttributesRenameTogetherWithMatchingFormatPatternSucceeds(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50)))))

    for {
      columnId <- createLinkColumn(
        Json.arr(percentageAttribute()),
        formatPattern = Some("{{value}} ({{attributes.percentage}}%)")
      )
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj(
          "linkAttributes" -> Json.arr(percentageAttribute(name = "share")),
          "formatPattern" -> "{{value}} ({{attributes.share}}%)"
        )
      )
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals("share", column.getJsonArray("linkAttributes").getJsonObject(0).getString("name"))
      assertEquals("{{value}} ({{attributes.share}}%)", column.getString("formatPattern"))
      // the rename is cosmetic, the stored value stays put
      assertEquals(50, cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getInteger(0))
    }
  }

  // A pattern that never referenced an attribute can't be invalidated by renaming one.
  @Test
  def changeLinkAttributesRenameWithValueOnlyFormatPatternSucceeds(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute()), formatPattern = Some("{{value}}"))
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(name = "share")))
      )
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertEquals("share", column.getJsonArray("linkAttributes").getJsonObject(0).getString("name"))
      assertEquals("{{value}}", column.getString("formatPattern"))
    }
  }

  // Clearing the definitions is fine as long as no pattern depends on them.
  @Test
  def clearingLinkAttributesWithoutFormatPatternSucceeds(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute()))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId", Json.obj("linkAttributes" -> Json.arr()))
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertNull(column.getJsonArray("linkAttributes"))
    }
  }

  // ... and when one does depend on them, clearing both together is the way out. `formatPattern: null` deletes the
  // pattern (as opposed to omitting the key, which leaves it untouched) - without that, a link column that once had
  // a pattern referencing an attribute could never get rid of its definitions again.
  @Test
  def clearingLinkAttributesTogetherWithFormatPatternSucceeds(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50)))))

    for {
      columnId <- createLinkColumn(
        Json.arr(percentageAttribute()),
        formatPattern = Some("{{value}} ({{attributes.percentage}}%)")
      )
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(), "formatPattern" -> null)
      )
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertFalse(column.containsKey("linkAttributes"))
      assertFalse(column.containsKey("formatPattern"))
      // the definitions are gone, so the values stored under them have to be gone as well
      assertFalse(cell.getJsonArray("value").getJsonObject(0).containsKey("attributes"))
    }
  }

  // `linkAttributes: null` is the same request as `linkAttributes: []` - both mean "submitted, and empty".
  @Test
  def clearingLinkAttributesWithNullTogetherWithFormatPatternSucceeds(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(
        Json.arr(percentageAttribute()),
        formatPattern = Some("{{attributes.percentage}}")
      )
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> null, "formatPattern" -> null)
      )
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertFalse(column.containsKey("linkAttributes"))
      assertFalse(column.containsKey("formatPattern"))
    }
  }

  // Same as clearingLinkAttributesLeavingFormatPatternDanglingFails, but with null instead of an empty array: the
  // two spellings mean the same thing, so they have to be rejected the same way.
  @Test
  def clearingLinkAttributesWithNullLeavingFormatPatternDanglingFails(implicit c: TestContext): Unit =
    exceptionTest("unprocessable.entity") {
      for {
        columnId <- createLinkColumn(
          Json.arr(percentageAttribute()),
          formatPattern = Some("{{attributes.percentage}}")
        )
        _ <- sendRequest("POST", s"/tables/1/columns/$columnId", Json.obj("linkAttributes" -> null))
      } yield ()
    }

  // Deleting only the pattern is always safe - definitions without a pattern referencing them are a valid state.
  @Test
  def clearingOnlyFormatPatternKeepsLinkAttributes(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(
        Json.arr(percentageAttribute()),
        formatPattern = Some("{{value}} ({{attributes.percentage}}%)")
      )
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId", Json.obj("formatPattern" -> null))
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertFalse(column.containsKey("formatPattern"))
      assertJSONEquals(Json.arr(percentageAttribute()), column.getJsonArray("linkAttributes"))
    }
  }

  // Replacing the definitions while dropping the pattern in the same request: the pattern is checked as deleted,
  // not against the definitions being written.
  @Test
  def changeLinkAttributesTogetherWithClearedFormatPatternSucceeds(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(
        Json.arr(percentageAttribute()),
        formatPattern = Some("{{value}} ({{attributes.percentage}}%)")
      )
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(name = "share")), "formatPattern" -> null)
      )
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertFalse(column.containsKey("formatPattern"))
      assertEquals("share", column.getJsonArray("linkAttributes").getJsonObject(0).getString("name"))
    }
  }

  // Omitting formatPattern still means "leave it untouched" - deleting it has to be requested explicitly.
  @Test
  def omittingFormatPatternLeavesItUntouched(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute()), formatPattern = Some("{{value}}"))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId", Json.obj("name" -> "renamed"))
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertEquals("{{value}}", column.getString("formatPattern"))
    }
  }

  // Anything that is neither a string nor null can't be a pattern - and must not be silently ignored, which is what
  // would make a "delete" that was spelled wrong look like it worked.
  @Test
  def changeFormatPatternToNonStringFails(implicit c: TestContext): Unit =
    exceptionTest("error.json.formatPattern") {
      for {
        columnId <- createLinkColumn(Json.arr(percentageAttribute()))
        _ <- sendRequest("POST", s"/tables/1/columns/$columnId", Json.obj("formatPattern" -> 42))
      } yield ()
    }

  // ---------------------------------------------------------------------------------------------------------------
  // A multilanguage attribute value is keyed by langtag, so a table without langtags can't carry one. Allowing it
  // used to let the multilanguage reshape run with an empty langtag list, which replaced every stored value with an
  // empty object (false -> true) or a null (true -> false) and committed.
  // ---------------------------------------------------------------------------------------------------------------

  // `langtags: []` is an explicitly supported table configuration (see StructureRouter), not a degenerate one.
  // A column is needed because a link's target table must have at least one; rows are not.
  private def createTableWithoutLangtags(name: String): Future[TableId] = {
    for {
      tableId <- sendRequest("POST", "/tables", Json.obj("name" -> name, "langtags" -> Json.arr()))
        .map(_.getLong("id").toLong)
      _ <- sendRequest(
        "POST",
        s"/tables/$tableId/columns",
        Json.obj("columns" -> Json.arr(Json.obj("name" -> "text", "kind" -> "text", "identifier" -> true)))
      )
    } yield tableId
  }

  @Test
  def createLinkColumnWithMultilanguageAttributeOnTableWithoutLangtagsFails(implicit c: TestContext): Unit =
    exceptionTest("unprocessable.entity") {
      for {
        tableId <- createTableWithoutLangtags("No Langtags")
        toTableId <- createTableWithoutLangtags("No Langtags 2")
        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns",
          Json.obj("columns" -> Json.arr(Json.obj(
            "name" -> "Test Link 1",
            "kind" -> "link",
            "toTable" -> toTableId,
            "linkAttributes" -> Json.arr(percentageAttribute(multilanguage = true))
          )))
        )
      } yield ()
    }

  @Test
  def changeLinkColumnToMultilanguageAttributeOnTableWithoutLangtagsFails(implicit c: TestContext): Unit =
    exceptionTest("unprocessable.entity") {
      for {
        tableId <- createTableWithoutLangtags("No Langtags")
        toTableId <- createTableWithoutLangtags("No Langtags 2")
        columnId <- sendRequest(
          "POST",
          s"/tables/$tableId/columns",
          Json.obj("columns" -> Json.arr(Json.obj(
            "name" -> "Test Link 1",
            "kind" -> "link",
            "toTable" -> toTableId,
            "linkAttributes" -> Json.arr(percentageAttribute(multilanguage = false))
          )))
        ).map(_.getJsonArray("columns").getJsonObject(0).getLong("id").toLong)
        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/$columnId",
          Json.obj("linkAttributes" -> Json.arr(percentageAttribute(multilanguage = true)))
        )
      } yield ()
    }

  // A language-neutral attribute needs no langtags at all and stays allowed.
  @Test
  def createLinkColumnWithLanguageNeutralAttributeOnTableWithoutLangtagsSucceeds(implicit c: TestContext)
      : Unit = okTest {
    for {
      tableId <- createTableWithoutLangtags("No Langtags")
      toTableId <- createTableWithoutLangtags("No Langtags 2")
      result <- sendRequest(
        "POST",
        s"/tables/$tableId/columns",
        Json.obj("columns" -> Json.arr(Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "toTable" -> toTableId,
          "linkAttributes" -> Json.arr(percentageAttribute(multilanguage = false))
        )))
      )
    } yield {
      assertEquals(
        "percentage",
        result.getJsonArray(
          "columns"
        ).getJsonObject(0).getJsonArray("linkAttributes").getJsonObject(0).getString("name")
      )
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Collapsing multilanguage -> language-neutral walks the stored object, not the table's langtag list, because the
  // two can disagree: a langtag can be removed from the table after a value was stored under it.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  def changeLinkColumnMultilanguageTrueToFalseKeepsValueOfRemovedLangtag(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(Json.obj("de-DE" -> 50)))))
    )

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(multilanguage = true)))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)

      // de-DE is dropped from the table, so the only stored value now sits under a langtag the table
      // no longer knows - iterating table langtags to collapse would silently discard it
      _ <- sendRequest("POST", "/tables/1", Json.obj("langtags" -> Json.arr("en-GB")))

      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(multilanguage = false)))
      )
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals(50, cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getInteger(0))
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // description is part of a DisplayInfo just like displayName, and the definition JSON is what gets persisted -
  // dropping it on serialization loses it for good rather than just hiding it from the response.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  def linkAttributeDescriptionRoundtrips(implicit c: TestContext): Unit = okTest {
    val attributeWithDescription = Json.obj(
      "name" -> "percentage",
      "displayName" -> Json.obj("de-DE" -> "Prozentanteil"),
      "description" -> Json.obj("de-DE" -> "Anteil in Prozent", "en-GB" -> "Share in percent"),
      "kind" -> "integer",
      "multilanguage" -> false
    )

    for {
      columnId <- createLinkColumn(Json.arr(attributeWithDescription))
      afterCreate <- sendRequest("GET", s"/tables/1/columns/$columnId")
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(attributeWithDescription))
      )
      afterChange <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      val expected = Json.obj("de-DE" -> "Anteil in Prozent", "en-GB" -> "Share in percent")

      assertEquals(expected, afterCreate.getJsonArray("linkAttributes").getJsonObject(0).getJsonObject("description"))
      assertEquals(expected, afterChange.getJsonArray("linkAttributes").getJsonObject(0).getJsonObject("description"))
      assertEquals(
        Json.obj("de-DE" -> "Prozentanteil"),
        afterChange.getJsonArray("linkAttributes").getJsonObject(0).getJsonObject("displayName")
      )
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // A kind migration to datetime has to produce the same spelling the API hands out for a datetime value written
  // directly - see LinkAttributesTest.dateTimeValueIsNormalizedIdenticallyByWriteAndMigration.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  def changeLinkColumnKindMigrationToDateTimeNormalizesFormat(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj(
        "values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr("2020-01-01T13:00:00.000+01:00")))
      )
    )

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(kind = "text")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(kind = "datetime")))
      )
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals(
        "2020-01-01T12:00:00.000Z",
        cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getString(0)
      )
    }
  }

  @Test
  def changeLinkColumnKindMigrationToDateNormalizesFormat(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr("2020-01-01"))))
    )

    for {
      columnId <- createLinkColumn(Json.arr(percentageAttribute(kind = "text")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(kind = "date")))
      )
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals(
        "2020-01-01",
        cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getString(0)
      )
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // The multilanguage guard asks the LINK, not the addressed column: linkAttributes live in system_link_table and are
  // shared by both sides, so evaluating only the addressed table's langtags made the answer depend on which side the
  // request came through - and locked the backlink side out of editing a perfectly valid definition.
  // ---------------------------------------------------------------------------------------------------------------

  // table 1 has langtags, table 2 does not; the link therefore has a langtag context
  private def createLinkToTableWithoutLangtags(multilanguage: Boolean): Future[(TableId, ColumnId)] = {
    for {
      _ <- createDefaultTable()
      toTableId <- createTableWithoutLangtags("No Langtags")
      // createTableWithoutLangtags deliberately creates no rows; the flip test links to one
      _ <- sendRequest("POST", s"/tables/$toTableId/rows", Json.obj())
      columnId <- sendRequest(
        "POST",
        "/tables/1/columns",
        Json.obj("columns" -> Json.arr(Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "toTable" -> toTableId,
          "singleDirection" -> false,
          "linkAttributes" -> Json.arr(percentageAttribute(multilanguage = multilanguage))
        )))
      ).map(_.getJsonArray("columns").getJsonObject(0).getLong("id").toLong)
    } yield (toTableId, columnId)
  }

  @Test
  def createMultilanguageLinkAttributeSucceedsWhenOnlyOneSideHasLangtags(implicit c: TestContext): Unit = okTest {
    for {
      (_, columnId) <- createLinkToTableWithoutLangtags(multilanguage = true)
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertTrue(column.getJsonArray("linkAttributes").getJsonObject(0).getBoolean("multilanguage"))
    }
  }

  @Test
  def changeLinkAttributesFromBacklinkSideWithoutOwnLangtagsSucceeds(implicit c: TestContext): Unit = okTest {
    for {
      (toTableId, _) <- createLinkToTableWithoutLangtags(multilanguage = true)
      backlinkId <- sendRequest("GET", s"/tables/$toTableId/columns").map(
        _.getJsonArray("columns")
          .asScala
          .map(_.asInstanceOf[JsonObject])
          .collectFirst({ case col if col.getString("kind") == "link" => col.getLong("id").toLong })
          .get
      )
      // a pure rename from the side that has no langtags of its own
      _ <- sendRequest(
        "POST",
        s"/tables/$toTableId/columns/$backlinkId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(multilanguage = true, name = "share")))
      )
      forwardColumn <- sendRequest("GET", "/tables/1/columns")
    } yield {
      val linkColumn = forwardColumn.getJsonArray("columns")
        .asScala
        .map(_.asInstanceOf[JsonObject])
        .collectFirst({ case col if col.getString("kind") == "link" => col })
        .get

      assertEquals("share", linkColumn.getJsonArray("linkAttributes").getJsonObject(0).getString("name"))
    }
  }

  // The multilanguage flip must target the link's langtags too, not just the addressed table's - otherwise a value
  // flipped from the langtag-less side would end up in an empty object.
  @Test
  def multilanguageFlipFromBacklinkSideUsesTheLinksLangtags(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50)))))

    for {
      (toTableId, columnId) <- createLinkToTableWithoutLangtags(multilanguage = false)
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
      backlinkId <- sendRequest("GET", s"/tables/$toTableId/columns").map(
        _.getJsonArray("columns")
          .asScala
          .map(_.asInstanceOf[JsonObject])
          .collectFirst({ case col if col.getString("kind") == "link" => col.getLong("id").toLong })
          .get
      )
      _ <- sendRequest(
        "POST",
        s"/tables/$toTableId/columns/$backlinkId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(multilanguage = true)))
      )
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      val attributeValue = cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getJsonObject(0)

      assertEquals(50, attributeValue.getInteger("de-DE"))
      assertEquals(50, attributeValue.getInteger("en-GB"))
    }
  }

}
