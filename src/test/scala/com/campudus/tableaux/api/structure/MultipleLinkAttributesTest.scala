package com.campudus.tableaux.api.structure

import com.campudus.tableaux.database.domain.LinkAttributeDefinition
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.helper.Json
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.json.{JsonArray, JsonObject}

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.junit.runner.RunWith

/**
  * `linkAttributes` is capped at [[LinkAttributeDefinition.defaultMaxCount]] entries per link column, but nothing below
  * that cap is written for exactly one definition: values are positional arrays, definition changes migrate them slot
  * by slot, and a formatPattern addresses a definition by name. With the cap hard-coded at 1 all of that is unreachable
  * code that no test can tell apart from code that doesn't work - so this test class raises the cap for itself (and
  * only for itself, see the `@After`) and exercises the N-definition paths for real.
  *
  * Everything here is about the second and third definition. Single-definition behaviour is covered by
  * ChangeLinkAttributesStructureTest (structure) and LinkAttributesTest (values), including the fact that the cap is
  * enforced at all - which is why the one cap test below is about the raised cap still being a cap.
  */
@RunWith(classOf[VertxUnitRunner])
class MultipleLinkAttributesTest extends TableauxTestBase {

  private val raisedMaxCount = 3

  @Before
  def raiseLinkAttributeMaxCount(): Unit = LinkAttributeDefinition.setMaxCountForTest(raisedMaxCount)

  // Without this the raised cap leaks into every test class that runs after this one in the same JVM
  @After
  def resetLinkAttributeMaxCount(): Unit = LinkAttributeDefinition.resetMaxCountForTest()

  private def attribute(
      name: String,
      kind: String = "integer",
      multilanguage: Boolean = false
  ): JsonObject = {
    Json.obj(
      "name" -> name,
      "displayName" -> Json.obj("de-DE" -> s"Attribut $name"),
      "kind" -> kind,
      "multilanguage" -> multilanguage
    )
  }

  private def createLinkColumn(
      linkAttributes: JsonArray,
      formatPattern: Option[String] = None
  ): Future[ColumnId] = {
    val baseJson = Json.obj(
      "name" -> "Test Link 1",
      "kind" -> "link",
      "toTable" -> 2,
      // explicit rather than relying on the default - the backlink column actually being created is
      // load-bearing for the backlink assertions further down
      "singleDirection" -> false,
      "linkAttributes" -> linkAttributes
    )
    val columnJson = formatPattern match {
      case Some(pattern) => baseJson.mergeIn(Json.obj("formatPattern" -> pattern))
      case None => baseJson
    }

    for {
      _ <- createDefaultTable()
      _ <- createDefaultTable("Test Table 2", 2)
      result <- sendRequest("POST", "/tables/1/columns", Json.obj("columns" -> Json.arr(columnJson)))
    } yield result.getJsonArray("columns").getJsonObject(0).getLong("id").toLong
  }

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

  private def putLink(attributes: JsonArray, toRow: RowId = 1): JsonObject = {
    Json.obj("value" -> Json.obj("values" -> Json.arr(Json.obj("id" -> toRow, "attributes" -> attributes))))
  }

  private def changeLinkAttributes(columnId: ColumnId, linkAttributes: JsonArray): Future[JsonObject] = {
    sendRequest("POST", s"/tables/1/columns/$columnId", Json.obj("linkAttributes" -> linkAttributes))
  }

  private def attributesOfFirstLink(cell: JsonObject): JsonArray = {
    cell.getJsonArray("value").getJsonObject(0).getJsonArray("attributes")
  }

  private def retrieveAttributes(columnId: ColumnId): Future[JsonArray] = {
    sendRequest("GET", s"/tables/1/columns/$columnId/rows/1").map(attributesOfFirstLink)
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Definition
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  def createLinkColumnWithSeveralLinkAttributes(implicit c: TestContext): Unit = okTest {
    val definitions = Json.arr(
      attribute("percentage"),
      attribute("note", kind = "text"),
      attribute("since", kind = "date")
    )

    for {
      columnId <- createLinkColumn(definitions)
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      // order is the contract, not just the content: it is what a value array is positional against
      assertJSONEquals(definitions, column.getJsonArray("linkAttributes"))
    }
  }

  // The raised cap is still a cap - the seam moves the limit, it doesn't remove the check.
  @Test
  def createLinkColumnAboveTheRaisedCapFails(implicit c: TestContext): Unit =
    exceptionTest("error.json.linkAttributes") {
      val definitions = Json.arr(
        attribute("a"),
        attribute("b"),
        attribute("c"),
        attribute("d")
      )

      createLinkColumn(definitions)
    }

  // Unreachable while the cap is 1 (two entries fail the size check first), so this is the first test that can see
  // it: a value is addressed by name in a formatPattern, which duplicates make unresolvable.
  @Test
  def createLinkColumnWithDuplicateLinkAttributeNamesFails(implicit c: TestContext): Unit =
    exceptionTest("error.json.linkAttributes") {
      createLinkColumn(Json.arr(attribute("percentage"), attribute("percentage", kind = "text")))
    }

  @Test
  def changeLinkColumnToDuplicateLinkAttributeNamesFails(implicit c: TestContext): Unit =
    exceptionTest("error.json.linkAttributes") {
      for {
        columnId <- createLinkColumn(Json.arr(attribute("percentage")))
        _ <- changeLinkAttributes(columnId, Json.arr(attribute("percentage"), attribute("percentage")))
      } yield ()
    }

  @Test
  def formatPatternCanReferenceEveryLinkAttribute(implicit c: TestContext): Unit = okTest {
    val pattern = "{{value}}: {{attributes.percentage}}% ({{attributes.note}})"

    for {
      columnId <- createLinkColumn(
        Json.arr(attribute("percentage"), attribute("note", kind = "text")),
        formatPattern = Some(pattern)
      )
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
    } yield {
      assertEquals(pattern, column.getString("formatPattern"))
    }
  }

  // Only the second definition is renamed, so only the second token dangles - the check has to look at all of them,
  // not just the first.
  @Test
  def renamingAnAttributeReferencedByTheFormatPatternFails(implicit c: TestContext): Unit =
    exceptionTest("unprocessable.entity") {
      for {
        columnId <- createLinkColumn(
          Json.arr(attribute("percentage"), attribute("note", kind = "text")),
          formatPattern = Some("{{attributes.percentage}} ({{attributes.note}})")
        )
        _ <- changeLinkAttributes(columnId, Json.arr(attribute("percentage"), attribute("comment", kind = "text")))
      } yield ()
    }

  // ---------------------------------------------------------------------------------------------------------------
  // Values
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  def attributeValuesRoundtripPositionallyForEveryDefinition(implicit c: TestContext): Unit = okTest {
    val definitions = Json.arr(
      attribute("percentage"),
      attribute("note", kind = "text"),
      attribute("checked", kind = "boolean")
    )
    val values = Json.arr(50, "some note", true)

    for {
      columnId <- createLinkColumn(definitions)
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(values))
      backlinkColumnId <- findBacklinkColumnId(toTable = 1)

      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
      backlinkCell <- sendRequest("GET", s"/tables/2/columns/$backlinkColumnId/rows/1")
    } yield {
      assertEquals(values, attributesOfFirstLink(cell))
      // values live once per edge, so all three come back identically from the other side
      assertEquals(values, attributesOfFirstLink(backlinkCell))
    }
  }

  // A value array is parallel to the definitions, so a missing trailing value is not "the rest is empty" - there
  // would be no way to tell which definition the values that were sent belong to.
  @Test
  def attributeValuesMustCoverEveryDefinition(implicit c: TestContext): Unit =
    exceptionTest("error.json.link-attributes") {
      for {
        columnId <- createLinkColumn(Json.arr(attribute("percentage"), attribute("note", kind = "text")))
        _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(Json.arr(50)))
      } yield ()
    }

  // Type checking is per slot, so the second value has to be checked against the second definition and not against
  // whatever the first one happens to allow.
  @Test
  def attributeValuesAreCheckedAgainstTheirOwnDefinition(implicit c: TestContext): Unit =
    exceptionTest("error.json.link-attributes") {
      for {
        columnId <- createLinkColumn(Json.arr(attribute("note", kind = "text"), attribute("percentage")))
        _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(Json.arr("a note", "not a number")))
      } yield ()
    }

  @Test
  def clearedAndFilledSlotsCoexist(implicit c: TestContext): Unit = okTest {
    val definitions = Json.arr(attribute("percentage"), attribute("note", kind = "text"))
    val values = Json.arr().addNull().add("some note")

    for {
      columnId <- createLinkColumn(definitions)
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(values))
      attributes <- retrieveAttributes(columnId)
    } yield {
      // the cleared slot keeps its position instead of collapsing the array onto the filled one
      assertEquals(values, attributes)
    }
  }

  @Test
  def multilanguageAndSingleLanguageAttributesCoexist(implicit c: TestContext): Unit = okTest {
    val definitions = Json.arr(
      attribute("percentage"),
      attribute("note", kind = "text", multilanguage = true)
    )
    val values = Json.arr(50, Json.obj("de-DE" -> "Notiz", "en-GB" -> "note"))

    for {
      columnId <- createLinkColumn(definitions)
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(values))
      attributes <- retrieveAttributes(columnId)
    } yield {
      assertEquals(values, attributes)
    }
  }

  @Test
  def putLinkAttributesEndpointWritesEverySlot(implicit c: TestContext): Unit = okTest {
    val definitions = Json.arr(attribute("percentage"), attribute("note", kind = "text"))

    for {
      columnId <- createLinkColumn(definitions)
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(Json.arr(50, "some note")))
      _ <- sendRequest(
        "PUT",
        s"/tables/1/columns/$columnId/rows/1/link/1/attributes",
        Json.obj("attributes" -> Json.arr(75, "another note"))
      )
      attributes <- retrieveAttributes(columnId)
    } yield {
      assertEquals(Json.arr(75, "another note"), attributes)
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Definition changes: every migration addresses one slot, so the ones it doesn't address have to stay untouched
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  def changingTheKindOfOneAttributeMigratesOnlyItsSlot(implicit c: TestContext): Unit = okTest {
    val definitions = Json.arr(
      attribute("percentage"),
      attribute("amount", kind = "text"),
      attribute("note", kind = "text")
    )

    for {
      columnId <- createLinkColumn(definitions)
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(Json.arr(50, "12", "some note")))
      _ <- changeLinkAttributes(
        columnId,
        Json.arr(attribute("percentage"), attribute("amount", kind = "numeric"), attribute("note", kind = "text"))
      )
      attributes <- retrieveAttributes(columnId)
    } yield {
      assertEquals(50, attributes.getInteger(0))
      assertEquals(12.0, attributes.getValue(1).asInstanceOf[Number].doubleValue(), 0.001)
      assertEquals("some note", attributes.getString(2))
    }
  }

  @Test
  def flippingMultilanguageOnOneAttributeReshapesOnlyItsSlot(implicit c: TestContext): Unit = okTest {
    val definitions = Json.arr(attribute("percentage"), attribute("note", kind = "text"))

    for {
      columnId <- createLinkColumn(definitions)
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(Json.arr(50, "some note")))
      _ <- changeLinkAttributes(
        columnId,
        Json.arr(attribute("percentage"), attribute("note", kind = "text", multilanguage = true))
      )
      attributes <- retrieveAttributes(columnId)
    } yield {
      assertEquals(50, attributes.getInteger(0))
      assertEquals(Json.obj("de-DE" -> "some note", "en-GB" -> "some note"), attributes.getJsonObject(1))
    }
  }

  // A rename is cosmetic: definitions are diffed by position, so slot 1 stays slot 1 and keeps its value.
  @Test
  def renamingOneOfSeveralAttributesKeepsEveryValueInPlace(implicit c: TestContext): Unit = okTest {
    val definitions = Json.arr(attribute("percentage"), attribute("note", kind = "text"))

    for {
      columnId <- createLinkColumn(definitions)
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(Json.arr(50, "some note")))
      _ <- changeLinkAttributes(columnId, Json.arr(attribute("percentage"), attribute("comment", kind = "text")))
      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      attributes <- retrieveAttributes(columnId)
    } yield {
      assertEquals("comment", column.getJsonArray("linkAttributes").getJsonObject(1).getString("name"))
      assertEquals(Json.arr(50, "some note"), attributes)
    }
  }

  // Adding a definition to a link that already carries values: the stored array is parallel to the definitions, so
  // it has to grow a cleared slot. Without that it stays short, and the value the API hands out is one the API then
  // refuses as a write - which is what the round-trip at the end pins down.
  @Test
  def addingAnAttributePadsAlreadyStoredValues(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(Json.arr(attribute("percentage")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(Json.arr(50)))
      _ <- changeLinkAttributes(columnId, Json.arr(attribute("percentage"), attribute("note", kind = "text")))

      attributes <- retrieveAttributes(columnId)

      // what was just read has to be acceptable as a write again (read-modify-write): a stored array left at the
      // old length reads back one value short, and one value per definition is exactly what a write insists on
      _ <- sendRequest(
        "PUT",
        s"/tables/1/columns/$columnId/rows/1/link/1/attributes",
        Json.obj("attributes" -> attributes)
      )
      attributesAfterRewrite <- retrieveAttributes(columnId)
    } yield {
      assertEquals(Json.arr().add(50).addNull(), attributes)
      assertEquals(Json.arr().add(50).addNull(), attributesAfterRewrite)
    }
  }

  // Links written before the added definition and links written after it end up with the same shape - the padding
  // must not leave two generations of value arrays side by side.
  @Test
  def addingAnAttributeLeavesOldAndNewLinksWithTheSameShape(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(Json.arr(attribute("percentage")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(Json.arr(50), toRow = 1))
      _ <- changeLinkAttributes(columnId, Json.arr(attribute("percentage"), attribute("note", kind = "text")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/2", putLink(Json.arr(75, "fresh"), toRow = 2))

      migratedCell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
      freshCell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/2")
    } yield {
      assertEquals(Json.arr().add(50).addNull(), attributesOfFirstLink(migratedCell))
      assertEquals(Json.arr(75, "fresh"), attributesOfFirstLink(freshCell))
    }
  }

  // Removing the trailing definition drops exactly its values; the remaining slot keeps its own.
  @Test
  def removingTheLastAttributeKeepsTheRemainingValues(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(Json.arr(attribute("percentage"), attribute("note", kind = "text")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(Json.arr(50, "some note")))
      _ <- changeLinkAttributes(columnId, Json.arr(attribute("percentage")))

      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      attributes <- retrieveAttributes(columnId)

      // the shortened array is what the API accepts as a write now
      _ <- sendRequest(
        "PUT",
        s"/tables/1/columns/$columnId/rows/1/link/1/attributes",
        Json.obj("attributes" -> attributes)
      )
      attributesAfterRewrite <- retrieveAttributes(columnId)
    } yield {
      assertEquals(1, column.getJsonArray("linkAttributes").size())
      assertEquals(Json.arr(50), attributes)
      assertEquals(Json.arr(50), attributesAfterRewrite)
    }
  }

  // Clearing the definitions of a column that has several of them wipes every value, not just the first slot.
  @Test
  def clearingAllAttributesWipesEveryValue(implicit c: TestContext): Unit = okTest {
    for {
      columnId <- createLinkColumn(Json.arr(attribute("percentage"), attribute("note", kind = "text")))
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(Json.arr(50, "some note")))
      _ <- changeLinkAttributes(columnId, Json.arr())

      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      cell <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertFalse(column.containsKey("linkAttributes"))
      assertFalse(cell.getJsonArray("value").getJsonObject(0).containsKey("attributes"))
    }
  }

  // An incompatible cast in any slot fails the whole change, and the transaction rolls it back - the definitions of
  // the other slots must not be left applied to values that were never migrated.
  @Test
  def anImpossibleCastInOneSlotRollsBackTheWholeChange(implicit c: TestContext): Unit = okTest {
    val definitions = Json.arr(attribute("percentage"), attribute("note", kind = "text"))

    for {
      columnId <- createLinkColumn(definitions)
      _ <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", putLink(Json.arr(50, "not a number")))

      // second slot can't be cast to numeric, first one could
      failed <- sendRequest(
        "POST",
        s"/tables/1/columns/$columnId",
        Json.obj(
          "linkAttributes" -> Json.arr(attribute("percentage", kind = "text"), attribute("note", kind = "numeric"))
        )
      ).map(_ => false).recover({ case _ => true })

      column <- sendRequest("GET", s"/tables/1/columns/$columnId")
      attributes <- retrieveAttributes(columnId)
    } yield {
      assertTrue("changing the definition to an impossible kind must fail", failed)
      assertJSONEquals(definitions, column.getJsonArray("linkAttributes"))
      assertEquals(Json.arr(50, "not a number"), attributes)
    }
  }
}
