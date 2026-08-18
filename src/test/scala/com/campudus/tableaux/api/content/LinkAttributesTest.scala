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

  private def percentageAttribute(multilanguage: Boolean = false, kind: String = "integer"): JsonObject = {
    Json.obj(
      "name" -> "percentage",
      "displayName" -> Json.obj("de-DE" -> "Prozentanteil"),
      "kind" -> kind,
      "multilanguage" -> multilanguage
    )
  }

  private def postLinkColWithAttributes(
      toTableId: TableId,
      name: String = "Test Link 1",
      multilanguage: Boolean = false,
      kind: String = "integer",
      singleDirection: Boolean = false
  ) = {
    Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> name,
          "kind" -> "link",
          "toTable" -> toTableId,
          "singleDirection" -> singleDirection,
          "linkAttributes" -> Json.arr(percentageAttribute(multilanguage, kind))
        )
      )
    )
  }

  private def createLinkColumnWithAttributes(
      tableId: TableId,
      toTableId: TableId,
      multilanguage: Boolean = false,
      kind: String = "integer",
      name: String = "Test Link 1",
      singleDirection: Boolean = false
  ): Future[ColumnId] = {
    sendRequest(
      "POST",
      s"/tables/$tableId/columns",
      postLinkColWithAttributes(
        toTableId,
        name = name,
        multilanguage = multilanguage,
        kind = kind,
        singleDirection = singleDirection
      )
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
  def createLinkWithAttributesUsingToShape(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj("to" -> 1, "attributes" -> Json.arr(50))
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
  def rejectAttributesOnToShapeOnColumnWithoutDefinition(implicit c: TestContext): Unit =
    exceptionTest("error.json.link-attributes") {
      val putLink = Json.obj(
        "value" -> Json.obj("to" -> 1, "attributes" -> Json.arr(50))
      )

      for {
        _ <- setupTwoTables()
        linkColumnId <- createLinkColumn(1, 2, singleDirection = false)
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      } yield ()
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

  // a langtag explicitly set to null is a value ("cleared in this language"), not an absence - it has to survive
  // the read path, which is why RowModel.generateLinkProjection merges attributes outside of jsonb_strip_nulls
  // (that one works recursively and would drop the langtag key from the response)
  @Test
  def multilanguageAttributeKeepsExplicitlyNullLangtag(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj(
        "values" -> Json.arr(
          Json.obj("id" -> 1, "attributes" -> Json.arr(Json.obj("de-DE" -> null, "en-GB" -> "sixty")))
        )
      )
    )

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2, multilanguage = true, kind = "text")
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      cell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
      row <- sendRequest("GET", s"/tables/1/rows/1")
    } yield {
      val cellAttributeValue = cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getJsonObject(0)
      assertTrue(cellAttributeValue.containsKey("de-DE"))
      assertNull(cellAttributeValue.getValue("de-DE"))
      assertEquals("sixty", cellAttributeValue.getString("en-GB"))

      // same projection feeds the row endpoint - link column is the last one of the default table plus the link
      val rowLinkValue = row.getJsonArray("values").getJsonArray(2).getJsonObject(0)
      val rowAttributeValue = rowLinkValue.getJsonArray("attributes").getJsonObject(0)
      assertTrue(rowAttributeValue.containsKey("de-DE"))
      assertNull(rowAttributeValue.getValue("de-DE"))
      assertEquals("sixty", rowAttributeValue.getString("en-GB"))
    }
  }

  // clearing a single langtag is legal for every kind - a multilanguage value is rarely filled in for all languages
  // at once, and a value read back from the API (where a cleared langtag shows up as null) has to be writable again
  @Test
  def multilanguageAttributeAcceptsClearedLangtagForEveryKind(implicit c: TestContext): Unit = okTest {
    val valuePerKind: Seq[(String, Any)] = Seq(
      "text" -> "sixty",
      "integer" -> 50,
      "numeric" -> 12.5,
      "boolean" -> true,
      "date" -> "2026-08-18",
      "datetime" -> "2026-08-18T12:00:00.000Z"
    )

    for {
      _ <- setupTwoTables()
      _ <- valuePerKind.foldLeft(Future.successful(()))({
        case (previousKind, (kind, value)) =>
          val putLink = Json.obj(
            "value" -> Json.obj(
              "values" -> Json.arr(
                Json.obj("id" -> 1, "attributes" -> Json.arr(Json.obj("de-DE" -> null, "en-GB" -> value)))
              )
            )
          )

          for {
            _ <- previousKind
            // single direction only because every bidirectional link would add a backlink column to table 2, all
            // named after table 1 - one column per kind on the same pair of tables is the point of this test
            columnId <- createLinkColumnWithAttributes(
              1,
              2,
              multilanguage = true,
              kind = kind,
              name = s"Link $kind",
              singleDirection = true
            )
            _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink)
            cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
          } yield {
            val attributeValue =
              cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getJsonObject(0)
            assertNull(s"cleared langtag of kind $kind", attributeValue.getValue("de-DE"))
            assertEquals(s"value of kind $kind", value, attributeValue.getValue("en-GB"))
          }
      })
    } yield ()
  }

  @Test
  def multilanguageAttributeWithClearedLangtagRoundtrips(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj(
        "values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(Json.obj("de-DE" -> null, "en-GB" -> 50))))
      )
    )

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2, multilanguage = true)
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      cell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
      // writing back verbatim what was just read must be accepted, not rejected as a type violation
      _ <- sendRequest(
        "PUT",
        s"/tables/1/columns/$linkColumnId/rows/1/link/1/attributes",
        Json.obj("attributes" -> cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes"))
      )
      afterRoundtrip <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
    } yield {
      assertEquals(cell, afterRoundtrip)
    }
  }

  // an attribute value stored as JSON null must not be confused with "no attributes stored at all" (SQL NULL):
  // the first one keeps its slot in the positional array, only the latter drops the key from the response
  @Test
  def nullAttributeValueKeepsItsSlot(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr().addNull())))
    )

    val expected = Json.obj(
      "status" -> "ok",
      "value" -> Json.arr(Json.obj("id" -> 1, "value" -> "table2row1", "attributes" -> Json.arr().addNull()))
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

  // ---------------------------------------------------------------------------------------------------------------
  // Langtag keys are deliberately NOT restricted to the addressed table's langtags. A value belongs to the link,
  // which two tables with differing langtag sets share, and a langtag can be removed from a table after a value was
  // stored under it - so rejecting an unknown key would make a value the API hands out unwritable.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  def attributeValueUnderForeignLangtagIsAccepted(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj(
        "values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(Json.obj("kling-ON" -> 50))))
      )
    )

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2, multilanguage = true)
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      cell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
    } yield {
      val value = cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getJsonObject(0)
      assertEquals(50, value.getInteger("kling-ON"))
    }
  }

  /**
    * The round-trip invariant: whatever the API hands out has to be acceptable as a write again. Rejecting langtag keys
    * broke this for a value stored under a langtag that was later removed from the table - both the explicit attributes
    * endpoint and duplicateRow (which re-writes the values it just read) failed with 400.
    */
  @Test
  def attributeValueUnderRemovedLangtagStaysWritable(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(Json.obj("de-DE" -> 50)))))
    )

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2, multilanguage = true)
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)

      // de-DE is no longer one of the table's langtags, but the stored value still sits under it
      _ <- sendRequest("POST", "/tables/1", Json.obj("langtags" -> Json.arr("en-GB")))

      cell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
      readBack = cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes")

      // writing back exactly what was read must work
      _ <- sendRequest(
        "PUT",
        s"/tables/1/columns/$linkColumnId/rows/1/link/1/attributes",
        Json.obj("attributes" -> readBack)
      )

      // duplicateRow does the same round-trip internally
      duplicated <- sendRequest("POST", "/tables/1/rows/1/duplicate")
      duplicatedCell <-
        sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/${duplicated.getNumber("id")}")
    } yield {
      assertEquals(Json.obj("de-DE" -> 50), readBack.getJsonObject(0))
      assertEquals(
        Json.obj("de-DE" -> 50),
        duplicatedCell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getJsonObject(0)
      )
    }
  }

  /**
    * Same invariant, reached without anyone reconfiguring anything: two linked tables with different langtag sets. The
    * value is written from the side that knows de-DE, then a row is duplicated on the side that does not.
    */
  @Test
  def attributeValueSurvivesDuplicateOnSideWithNarrowerLangtags(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(Json.obj("de-DE" -> 50)))))
    )

    for {
      _ <- createDefaultTable()
      toTableId <- sendRequest("POST", "/tables", Json.obj("name" -> "Narrow", "langtags" -> Json.arr("en-GB")))
        .map(_.getLong("id").toLong)
      _ <- sendRequest(
        "POST",
        s"/tables/$toTableId/columns",
        Json.obj("columns" -> Json.arr(Json.obj("name" -> "name", "kind" -> "text", "identifier" -> true)))
      )
      _ <- sendRequest("POST", s"/tables/$toTableId/rows", Json.obj())

      linkColumnId <- sendRequest(
        "POST",
        "/tables/1/columns",
        Json.obj("columns" -> Json.arr(Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "toTable" -> toTableId,
          "singleDirection" -> false,
          "linkAttributes" -> Json.arr(percentageAttribute(multilanguage = true))
        )))
      ).map(_.getJsonArray("columns").getJsonObject(0).getLong("id").toLong)

      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)

      // duplicate a row of the table whose langtags do not include de-DE
      duplicated <- sendRequest("POST", s"/tables/$toTableId/rows/1/duplicate")
    } yield {
      assertNotNull(duplicated.getNumber("id"))
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // date/datetime values are normalized on write, so one instant has exactly one stored spelling.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  def dateTimeValueIsNormalizedOnWrite(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj(
        "values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr("2020-01-01T13:00:00.000+01:00")))
      )
    )

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2, kind = "datetime")
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      cell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
    } yield {
      assertEquals(
        "2020-01-01T12:00:00.000Z",
        cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getString(0)
      )
    }
  }

  @Test
  def dateTimeValueIsNormalizedOnPutAttributesEndpoint(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj("values" -> Json.arr(Json.obj(
        "id" -> 1,
        "attributes" -> Json.arr("2019-01-01T00:00:00.000Z")
      )))
    )
    val putAttributes = Json.obj("attributes" -> Json.arr("2020-01-01T13:00:00.000+01:00"))

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2, kind = "datetime")
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1/link/1/attributes", putAttributes)
      cell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
    } yield {
      assertEquals(
        "2020-01-01T12:00:00.000Z",
        cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getString(0)
      )
    }
  }

  /**
    * Pins LinkAttributeValueValidator's Joda format to ModelHelper's Postgres TO_CHAR format. The two normalize the
    * same instant through completely different engines - Joda on write, TO_CHAR in ColumnModel's kind migration - so
    * nothing but a test comparing their output keeps them from drifting apart.
    */
  @Test
  def dateTimeValueIsNormalizedIdenticallyByWriteAndMigration(implicit c: TestContext): Unit = okTest {
    val sameInstant = "2020-06-15T14:30:45.123+02:00"
    val putLink =
      Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(sameInstant)))))

    def attributeValue(cell: JsonObject): String =
      cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getString(0)

    for {
      _ <- setupTwoTables()

      // two link columns between the same pair of tables, hence singleDirection - a bidirectional pair would
      // auto-create two backlink columns in table 2 that both derive their name from table 1
      //
      // written straight into a datetime attribute - normalized by Joda
      writtenColumnId <-
        createLinkColumnWithAttributes(1, 2, kind = "datetime", name = "Written Link", singleDirection = true)
      _ <- sendRequest("POST", s"/tables/1/columns/$writtenColumnId/rows/1", putLink)
      writtenCell <- sendRequest("GET", s"/tables/1/columns/$writtenColumnId/rows/1")

      // written as text, then migrated to datetime - normalized by Postgres
      migratedColumnId <-
        createLinkColumnWithAttributes(1, 2, kind = "text", name = "Migrated Link", singleDirection = true)
      _ <- sendRequest("POST", s"/tables/1/columns/$migratedColumnId/rows/1", putLink)
      _ <- sendRequest(
        "POST",
        s"/tables/1/columns/$migratedColumnId",
        Json.obj("linkAttributes" -> Json.arr(percentageAttribute(kind = "datetime")))
      )
      migratedCell <- sendRequest("GET", s"/tables/1/columns/$migratedColumnId/rows/1")
    } yield {
      assertEquals("2020-06-15T12:30:45.123Z", attributeValue(writtenCell))
      assertEquals(attributeValue(writtenCell), attributeValue(migratedCell))
    }
  }

  @Test
  def dateValueRoundtripsForDateKind(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj(
      "value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr("2020-01-01"))))
    )

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2, kind = "date")
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      cell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
    } yield {
      assertEquals("2020-01-01", cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getString(0))
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // An empty attributes array carries no value for any definition, so it means the same as sending none at all -
  // it must not put an `attributes` key on a column that has no definitions to interpret it.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  def emptyAttributesArrayOnColumnWithoutDefinitionsIsIgnored(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr()))))

    val expected = Json.obj(
      "status" -> "ok",
      "value" -> Json.arr(Json.obj("id" -> 1, "value" -> "table2row1"))
    )

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumn(1, 2, singleDirection = false)
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      cell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
    } yield {
      assertEquals(expected, cell)
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Duplicating a row round-trips its link values back through the write path, so attributes have to survive it.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  def duplicateRowKeepsLinkAttributes(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> 1, "attributes" -> Json.arr(50)))))

    for {
      _ <- setupTwoTables()
      linkColumnId <- createLinkColumnWithAttributes(1, 2)
      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      duplicated <- sendRequest("POST", "/tables/1/rows/1/duplicate")
      duplicatedCell <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/${duplicated.getNumber("id")}")
    } yield {
      assertEquals(
        50,
        duplicatedCell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes").getInteger(0)
      )
    }
  }

}
