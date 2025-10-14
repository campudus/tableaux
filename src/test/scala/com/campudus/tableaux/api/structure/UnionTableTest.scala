package com.campudus.tableaux.api.structure

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.Cardinality
import com.campudus.tableaux.database.domain.Constraint
import com.campudus.tableaux.database.model.TableauxModel.ColumnId
import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.testtools.RequestCreation._
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

sealed trait UnionTableTestHelper extends TableauxTestBase {

  def createTableJson(name: String) = Json.obj("name" -> name)

  def createTextColumnJson(name: String) = Columns().add(Identifier(TextCol(name))).getJson

  def createMultilangTextColumnJson(name: String) = Columns().add(Multilanguage(TextCol(name))).getJson

  def createNumberColumnJson(name: String, decimalDigits: Option[Int] = None) =
    Columns().add(NumericCol(name, decimalDigits)).getJson

  def createCardinalityLinkColumn(
      toTableId: TableId,
      name: String,
      from: Int,
      to: Int
  ) = Columns(LinkBiDirectionalCol(
    name,
    toTableId,
    Constraint(Cardinality(from, to), deleteCascade = false)
  )).getJson

  def fetchTable(tableId: TableId) = sendRequest("GET", s"/completetable/$tableId")

  def fetchColumns(tableId: TableId) = sendRequest("GET", s"/tables/$tableId/columns")

  // format: off
  /**
   * Creates three almost identical tables but with different column order.
   * 
   * - column "name" is of type text, single language
   * - column "color" is of type text, multi language
   * - column "prio" is of type number, single language
   * - column "glossLevel" is of type link to same common table (id: 1, name: glossLink)
   * 
   * The tables and their columns configuration is as follows:
   * 
   * glossLink (id: 1)
   * ┌───┬─────┐
   * │id │name │
   * ├───┼─────┤
   * │0  │1    │
   * └───┴─────┘
   * table1 (id: 2)
   * ┌───┬─────┬──────┬─────┬───────────┐
   * │id │name │color │prio │glossLevel │
   * ├───┼─────┼──────┼─────┼───────────┤
   * │0  │1    │2     │3    │4          │
   * └───┴─────┴──────┴─────┴───────────┘
   * table2 (id: 3)
   * ┌───┬─────┬──────┬─────┬───────────┐
   * │id │name │color │prio │glossLevel │
   * ├───┼─────┼──────┼─────┼───────────┤
   * │0  │1    │3     │4    │2          │
   * └───┴─────┴──────┴─────┴───────────┘
   * table3 (id: 4)
   * ┌───┬─────┬──────┬─────┬───────────┐
   * │id │name │color │prio │glossLevel │
   * ├───┼─────┼──────┼─────┼───────────┤
   * │0  │4    │2     │3    │1          │
   * └───┴─────┴──────┴─────┴───────────┘
   * union (id: 5)
   * ┌───┬─────┬──────┬─────┬───────────┐
   * │id │name │color │prio │glossLevel │
   * ├───┼─────┼──────┼─────┼───────────┤
   * │0  │4    │2     │3    │1          │
   * └───┴─────┴──────┴─────┴───────────┘
   */
  def createUnionTable(): Future[TableId] = {
  // format: on

    val createColumnName = createTextColumnJson("name")
    val createColumnColor = createMultilangTextColumnJson("color")
    val createColumnPrio = createNumberColumnJson("prio")

    for {
      glossLinkTableId <- createDefaultTable(name = "glossLink")
      createColumnGloss = createCardinalityLinkColumn(glossLinkTableId, "glossLevel", 1, 0)

      tableId1 <- sendRequest("POST", "/tables", createTableJson("table1")).map(_.getLong("id"))
      tableId2 <- sendRequest("POST", "/tables", createTableJson("table2")).map(_.getLong("id"))
      tableId3 <- sendRequest("POST", "/tables", createTableJson("table3")).map(_.getLong("id"))

      // create columns in tables in different order
      _ <- sendRequest("POST", s"/tables/$tableId1/columns", createColumnName)
      _ <- sendRequest("POST", s"/tables/$tableId1/columns", createColumnColor)
      _ <- sendRequest("POST", s"/tables/$tableId1/columns", createColumnPrio)
      _ <- sendRequest("POST", s"/tables/$tableId1/columns", createColumnGloss)

      _ <- sendRequest("POST", s"/tables/$tableId2/columns", createColumnName)
      _ <- sendRequest("POST", s"/tables/$tableId2/columns", createColumnGloss)
      _ <- sendRequest("POST", s"/tables/$tableId2/columns", createColumnColor)
      _ <- sendRequest("POST", s"/tables/$tableId2/columns", createColumnPrio)

      _ <- sendRequest("POST", s"/tables/$tableId3/columns", createColumnGloss)
      _ <- sendRequest("POST", s"/tables/$tableId3/columns", createColumnColor)
      _ <- sendRequest("POST", s"/tables/$tableId3/columns", createColumnPrio)
      _ <- sendRequest("POST", s"/tables/$tableId3/columns", createColumnName)

      payload = Json.obj(
        "name" -> "union",
        "type" -> "union",
        "displayName" -> Json.obj("de" -> "Union Table"),
        "originTables" -> Json.arr(tableId1, tableId3, tableId2)
      )
      resultUnionTable <- sendRequest("POST", "/tables", payload)

    } yield resultUnionTable.getLong("id")
  }
}

@RunWith(classOf[VertxUnitRunner])
class CreateUnionTableTest extends TableauxTestBase with UnionTableTestHelper {

  @Test
  def createUnionTable_withOriginTables_ok(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createUnionTable()
      unionTable <- sendRequest("GET", s"/completetable/$tableId")
    } yield {
      val expectedUnionTable = Json.obj(
        "name" -> "union",
        "type" -> "union",
        "displayName" -> Json.obj("de" -> "Union Table"),
        "originTables" -> Json.arr(2, 4, 3),
        "columns" -> Json.arr(
          Json.obj(
            "name" -> "originTable",
            "displayName" -> Json.obj("de" -> "Ursprungstabelle", "en" -> "Origin Table"),
            "description" -> Json.obj(
              "de" -> "Der Tabellenname, aus der die Daten stammen",
              "en" -> "The name of the table from which the data is taken"
            ),
            "kind" -> "text" // internally "origintable"
          )
        ),
        "rows" -> Json.emptyArr()
      )

      assertJSONEquals(expectedUnionTable, unionTable)
    }
  }

  @Test
  def createUnionTable_withoutOriginTables_shouldFail(implicit c: TestContext): Unit =
    exceptionTest("unprocessable.entity") {
      for {
        _ <- sendRequest(
          "POST",
          "/tables",
          Json.obj("name" -> "union", "type" -> "union", "displayName" -> Json.obj("de" -> "Union Table"))
        )
      } yield ()
    }

  @Test
  def createUnionTable_withInvalidOriginTables_shouldFail(implicit c: TestContext): Unit =
    exceptionTest("error.database.unknown") {
      for {
        _ <- sendRequest(
          "POST",
          "/tables",
          Json.obj(
            "name" -> "union",
            "type" -> "union",
            "displayName" -> Json.obj("de" -> "Union Table"),
            "originTables" -> Json.arr(999)
          )
        )
      } yield ()
    }
}

@RunWith(classOf[VertxUnitRunner])
class UpdateUnionTableTest extends TableauxTestBase with UnionTableTestHelper {

  @Test
  def updateUnionTable_addColumnsToUnionTable_ok(implicit c: TestContext): Unit = okTest {
    import scala.collection.JavaConverters._

    val unionTableCol1 = Json.obj(
      "name" -> "originTable",
      "kind" -> "text",
      "ordering" -> 1,
      "displayName" -> Json.obj("de" -> "Ursprungstabelle", "en" -> "Origin Table"),
      "description" -> Json.obj(
        "de" -> "Der Tabellenname, aus der die Daten stammen",
        "en" -> "The name of the table from which the data is taken"
      )
    )

    val unionTableCol2 = Json.obj(
      "name" -> "name",
      "kind" -> "text",
      "ordering" -> 1,
      "displayName" -> Json.obj("de" -> "Marketing Name", "en" -> "Marketing Name"),
      "description" -> Json.obj("de" -> "Marketingname der Farbe", "en" -> "marketing name of the color"),
      "originColumns" -> Json.obj(
        "2" -> Json.obj("id" -> 1),
        "3" -> Json.obj("id" -> 1),
        "4" -> Json.obj("id" -> 4)
      )
    )
    val unionTableCol3 = Json.obj(
      "name" -> "color",
      "kind" -> "text",
      "ordering" -> 2,
      "displayName" -> Json.obj("de" -> "Name der Farbe", "en" -> "Color Name"),
      "description" -> Json.obj("de" -> "Name der Farbe", "en" -> "Name of the color"),
      "originColumns" -> Json.obj(
        "2" -> Json.obj("id" -> 2),
        "3" -> Json.obj("id" -> 3),
        "4" -> Json.obj("id" -> 2)
      )
    )
    val unionTableCol4 = Json.obj(
      "name" -> "prio",
      "kind" -> "numeric",
      "ordering" -> 3,
      "displayName" -> Json.obj("de" -> "Prio", "en" -> "Priority"),
      "description" -> Json.obj("de" -> "Die Priorität der Farbe", "en" -> "The priority of the color"),
      "originColumns" -> Json.obj(
        "2" -> Json.obj("id" -> 3),
        "3" -> Json.obj("id" -> 4),
        "4" -> Json.obj("id" -> 3)
      )
    )
    val unionTableCol5 = Json.obj(
      "name" -> "glossLevel",
      "kind" -> "unionlink",
      "ordering" -> 3,
      "displayName" -> Json.obj("de" -> "Glanzgrad", "en" -> "Gloss Level"),
      "description" -> Json.obj("de" -> "Der Glanzgrad der Farbe", "en" -> "The gloss level of the color"),
      "originColumns" -> Json.obj(
        "2" -> Json.obj("id" -> 4),
        "3" -> Json.obj("id" -> 2),
        "4" -> Json.obj("id" -> 1)
      )
    )

    for {
      tableId <- createUnionTable()

      columns <- sendRequest(
        "POST",
        s"/tables/$tableId/columns",
        Json.obj("columns" -> Json.arr(unionTableCol2, unionTableCol3, unionTableCol4, unionTableCol5))
      )

      unionTable <- fetchTable(tableId)
      unionColumns <- fetchColumns(tableId).map(_.getJsonArray("columns")).map(_.asScala.collect({
        case obj: JsonObject => obj
      }).toSeq)
    } yield {
      val expectedTable = Json.obj(
        "id" -> 5,
        "name" -> "union",
        "hidden" -> false,
        "displayName" -> Json.obj("de" -> "Union Table"),
        "langtags" -> Json.arr("de-DE", "en-GB"),
        "type" -> "union",
        "originTables" -> Json.arr(2, 4, 3)
      )
      assertJSONEquals(expectedTable, unionTable)

      assertEquals(5, unionColumns.size)
      assertJSONEquals(unionTableCol1, unionColumns(0))
      assertJSONEquals(unionTableCol2, unionColumns(1))
      assertJSONEquals(unionTableCol3, unionColumns(2))
      assertJSONEquals(unionTableCol4, unionColumns(3))
      assertJSONEquals(unionTableCol5, unionColumns(4))
    }
  }

  @Test
  def updateUnionTable_changeName_ok(implicit c: TestContext): Unit = okTest {
    val payload = Json.obj("displayName" -> Json.obj("de" -> "Changed Union Table"))
    for {
      tableId <- createUnionTable()
      _ <- sendRequest("PATCH", s"/tables/$tableId", payload)
      unionTable <- fetchTable(tableId)
    } yield {
      val expectedTable = Json.obj(
        "id" -> 5,
        "name" -> "union",
        "hidden" -> false
      ).mergeIn(payload)
      assertJSONEquals(expectedTable, unionTable)
    }
  }

  @Test
  def updateUnionTable_changeOriginTables_shouldFail(implicit c: TestContext): Unit =
    exceptionTest("error.json.originTables") {
      val payload = Json.obj("originTables" -> Json.arr(1, 2, 3))
      for {
        tableId <- createUnionTable()
        _ <- sendRequest("PATCH", s"/tables/$tableId", payload)
      } yield ()
    }

  @Test
  def updateUnionTable_addColumnsToUnionTable_shouldFail(implicit c: TestContext): Unit =
    exceptionTest("error.json.originColumns") {
      val unionTableColWithoutOriginColumns = Json.obj(
        "name" -> "name",
        "kind" -> "text",
        "ordering" -> 1
        // "originColumns" -> Json.obj(
        //   "2" -> Json.obj("id" -> 1),
        //   "3" -> Json.obj("id" -> 1),
        //   "4" -> Json.obj("id" -> 4)
        // )
      )
      for {
        tableId <- createUnionTable()
        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns",
          Json.obj("columns" -> Json.arr(unionTableColWithoutOriginColumns))
        )
      } yield ()
    }
}

@RunWith(classOf[VertxUnitRunner])
class DeleteUnionTableTest extends TableauxTestBase with UnionTableTestHelper {

  @Test
  def deleteUnionTable_deleteColumnOriginTable_shouldFail(implicit c: TestContext): Unit =
    exceptionTest("error.database.delete-column-origintable") {

      val unionTableCol = Json.obj(
        "name" -> "name",
        "kind" -> "text",
        "ordering" -> 1,
        "description" -> Json.obj("en" -> "add a column so the column originTable is not the only one"),
        "originColumns" -> Json.obj(
          "2" -> Json.obj("id" -> 1),
          "3" -> Json.obj("id" -> 1),
          "4" -> Json.obj("id" -> 4)
        )
      )

      for {
        tableId <- createUnionTable()
        _ <- sendRequest("POST", s"/tables/$tableId/columns", Json.obj("columns" -> Json.arr(unionTableCol)))
        _ <- sendRequest("DELETE", s"/tables/$tableId/columns/1")
      } yield ()
    }

  @Test
  def deleteUnionTable_deleteUnionTable_ok(implicit c: TestContext): Unit =
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      val unionTableCol = Json.obj(
        "name" -> "name",
        "kind" -> "text",
        "ordering" -> 1,
        "description" -> Json.obj("en" -> "add a column so the column originTable is not the only one"),
        "originColumns" -> Json.obj(
          "2" -> Json.obj("id" -> 1),
          "3" -> Json.obj("id" -> 1),
          "4" -> Json.obj("id" -> 4)
        )
      )

      for {
        tableId <- createUnionTable()
        // add a column
        columns <- sendRequest("POST", s"/tables/$tableId/columns", Json.obj("columns" -> Json.arr(unionTableCol)))
        _ <- sendRequest("DELETE", s"/tables/$tableId")

        systemUnionTables <-
          dbConnection.query("SELECT count(*) FROM system_union_table").map(
            _.getJsonArray("results").getJsonArray(0).getInteger(0)
          )
        systemUnionColumns <-
          dbConnection.query("SELECT count(*) FROM system_union_column").map(
            _.getJsonArray("results").getJsonArray(0).getInteger(0)
          )
      } yield {
        assertEquals(0, systemUnionTables)
        assertEquals(0, systemUnionColumns)
      }
    }
}

// TODOs

// - deleteTable
// - history: table und column
// - data: row, rows, columns rows, first row
// - annotations: rows, columns, langtag
// - permissions: table row
// - content: `/tables/{tableId}/columns/{columnId}/rows/{rowId}` should not work, because on union table there is more than one row for that address
