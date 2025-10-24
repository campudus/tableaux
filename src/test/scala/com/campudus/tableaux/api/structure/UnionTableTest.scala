package com.campudus.tableaux.api.structure

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.Cardinality
import com.campudus.tableaux.database.domain.Constraint
import com.campudus.tableaux.database.model.TableauxModel.ColumnId
import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.testtools.RequestCreation._
import com.campudus.tableaux.testtools.TableauxTestBase
import com.campudus.tableaux.testtools.TestCustomException

import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.vertx.scala.core.json.Json

import scala.collection.JavaConverters._
import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

sealed trait UnionTableTestHelper extends TableauxTestBase {

  val unionTableCol1 = Json.obj(
    "name" -> "originTable",
    "kind" -> "origintable",
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
    "originColumns" -> Json.arr(
      Json.obj("tableId" -> 2, "columnId" -> 1),
      Json.obj("tableId" -> 3, "columnId" -> 1),
      Json.obj("tableId" -> 4, "columnId" -> 4)
    )
  )

  val unionTableCol3 = Json.obj(
    "name" -> "color",
    "kind" -> "text",
    "ordering" -> 2,
    "displayName" -> Json.obj("de" -> "Name der Farbe", "en" -> "Color Name"),
    "description" -> Json.obj("de" -> "Name der Farbe", "en" -> "Name of the color"),
    "originColumns" -> Json.arr(
      Json.obj("tableId" -> 2, "columnId" -> 2),
      Json.obj("tableId" -> 3, "columnId" -> 3),
      Json.obj("tableId" -> 4, "columnId" -> 2)
    )
  )

  val unionTableCol4 = Json.obj(
    "name" -> "prio",
    "kind" -> "numeric",
    "ordering" -> 3,
    "displayName" -> Json.obj("de" -> "Prio", "en" -> "Priority"),
    "description" -> Json.obj("de" -> "Die Priorität der Farbe", "en" -> "The priority of the color"),
    "originColumns" -> Json.arr(
      Json.obj("tableId" -> 2, "columnId" -> 3),
      Json.obj("tableId" -> 3, "columnId" -> 4),
      Json.obj("tableId" -> 4, "columnId" -> 3)
    )
  )

  val unionTableCol5 = Json.obj(
    "name" -> "glossLevel",
    "kind" -> "link",
    "ordering" -> 3,
    "displayName" -> Json.obj("de" -> "Glanzgrad", "en" -> "Gloss Level"),
    "description" -> Json.obj("de" -> "Der Glanzgrad der Farbe", "en" -> "The gloss level of the color"),
    "originColumns" -> Json.arr(
      Json.obj("tableId" -> 2, "columnId" -> 4),
      Json.obj("tableId" -> 3, "columnId" -> 2),
      Json.obj("tableId" -> 4, "columnId" -> 1)
    )
  )

  def createTableJson(name: String) =
    Json.obj("name" -> name, "displayName" -> Json.obj("de" -> s"${name}_de", "en" -> s"${name}_en"))

  def createTextColumnJson(name: String) = Identifier(TextCol(name))

  def createMultilangTextColumnJson(name: String) = Multilanguage(TextCol(name))

  def createNumberColumnJson(name: String, decimalDigits: Option[Int] = None) = NumericCol(name, decimalDigits)

  def createCardinalityLinkColumn(
      toTableId: TableId,
      name: String,
      from: Int,
      to: Int
  ) = LinkBiDirectionalCol(name, toTableId, Constraint(Cardinality(from, to), deleteCascade = false))

  def fetchTable(tableId: TableId) = sendRequest("GET", s"/completetable/$tableId")

  def fetchColumns(tableId: TableId) = sendRequest("GET", s"/tables/$tableId/columns")

  /**
    * Inserts test data into the three tables used for union table testing.
    *   - table2: 3 rows
    *   - table3: 5 rows
    *   - table4: 8 rows
    */
  def insertTestDataIntoTables(tableId2: TableId, tableId3: TableId, tableId4: TableId): Future[Unit] = {
    val table2ColumnsAndRows = Json.obj(
      "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2), Json.obj("id" -> 3), Json.obj("id" -> 4)),
      "rows" -> Json.arr(
        Json.obj("values" -> Json.arr("color1", Json.obj("de" -> "Rot", "en" -> "Red"), 1, Json.arr(1))),
        Json.obj("values" -> Json.arr("color2", Json.obj("de" -> "Blau", "en" -> "Blue"), 2, Json.arr(2))),
        Json.obj("values" -> Json.arr("color3", Json.obj("de" -> "Grün", "en" -> "Green"), 3, Json.arr(1)))
      )
    )

    val table3ColumnsAndRows = Json.obj(
      // "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 3), Json.obj("id" -> 4), Json.obj("id" -> 2)),
      // "rows" -> Json.arr(
      //   Json.obj("values" -> Json.arr("color12", Json.obj("de" -> "Rot", "en" -> "Red"), 1, Json.arr(1))),
      //   Json.obj("values" -> Json.arr("color13", Json.obj("de" -> "Blau", "en" -> "Blue"), 2, Json.emptyArr())),
      //   Json.obj("values" -> Json.arr("color14", Json.obj("de" -> "Grün", "en" -> "Green"), 3, Json.arr(1))),
      //   Json.obj("values" -> Json.arr("color15", Json.obj("de" -> "Gelb", "en" -> "Yellow"), 4, Json.emptyArr())),
      //   Json.obj("values" -> Json.arr("color16", Json.obj("de" -> "Schwarz", "en" -> "Black"), 5, Json.arr(1)))
      // )
      "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 3), Json.obj("id" -> 4), Json.obj("id" -> 2)),
      "rows" -> Json.arr(
        Json.obj("values" -> Json.arr("color12", Json.obj("de" -> "Rot", "en" -> "Red"), 1, Json.arr(1))),
        Json.obj("values" -> Json.arr("color13", Json.obj("de" -> "Blau", "en" -> "Blue"), 2, Json.emptyArr())),
        Json.obj("values" -> Json.arr("color14", Json.obj("de" -> "Grün", "en" -> "Green"), 3, Json.arr(1))),
        Json.obj("values" -> Json.arr("color15", Json.obj("de" -> "Gelb", "en" -> "Yellow"), 4, Json.emptyArr())),
        Json.obj("values" -> Json.arr("color16", Json.obj("de" -> "Schwarz", "en" -> "Black"), 5, Json.arr(1)))
      )
    )

    val table4ColumnsAndRows = Json.obj(
      "columns" -> Json.arr(Json.obj("id" -> 4), Json.obj("id" -> 2), Json.obj("id" -> 3), Json.obj("id" -> 1)),
      "rows" -> Json.arr(
        Json.obj("values" -> Json.arr("color4", Json.obj("de" -> "Rot", "en" -> "Red"), 1, Json.arr(1))),
        Json.obj("values" -> Json.arr("color5", Json.obj("de" -> "Blau", "en" -> "Blue"), 2, Json.emptyArr())),
        Json.obj("values" -> Json.arr("color6", Json.obj("de" -> "Grün", "en" -> "Green"), 3, Json.arr(2))),
        Json.obj("values" -> Json.arr("color7", Json.obj("de" -> "Gelb", "en" -> "Yellow"), 4, Json.emptyArr())),
        Json.obj("values" -> Json.arr("color8", Json.obj("de" -> "Schwarz", "en" -> "Black"), 5, Json.arr(2))),
        Json.obj("values" -> Json.arr("color9", Json.obj("de" -> "Weiß", "en" -> "White"), 6, Json.emptyArr())),
        Json.obj("values" -> Json.arr("color10", Json.obj("de" -> "Rosa", "en" -> "Pink"), 7, Json.arr(1))),
        Json.obj("values" -> Json.arr("color11", Json.obj("de" -> "Lila", "en" -> "Purple"), 8, Json.arr(2)))
      )
    )

    for {
      _ <- sendRequest("POST", s"/tables/$tableId2/rows", table2ColumnsAndRows)
      _ <- sendRequest("POST", s"/tables/$tableId3/rows", table3ColumnsAndRows)
      _ <- sendRequest("POST", s"/tables/$tableId4/rows", table4ColumnsAndRows)
    } yield ()
  }

  def addColumnsToUnionTable(tableId: TableId, columns: Seq[JsonObject]): Future[JsonObject] =
    sendRequest(
      "POST",
      s"/tables/$tableId/columns",
      Json.obj("columns" -> Json.arr(columns: _*))
    )

  

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
   * glossLink table1 (id: 1)
   * ┌───┬─────┐
   * │id │name │
   * ├───┼─────┤
   * │0  │1    │
   * └───┴─────┘
   * table2 (id: 2)
   * ┌───┬─────┬──────┬─────┬───────────┐
   * │id │name │color │prio │glossLevel │
   * ├───┼─────┼──────┼─────┼───────────┤
   * │0  │1    │2     │3    │4          │
   * └───┴─────┴──────┴─────┴───────────┘
   * table3 (id: 3)
   * ┌───┬─────┬──────┬─────┬───────────┐
   * │id │name │color │prio │glossLevel │
   * ├───┼─────┼──────┼─────┼───────────┤
   * │0  │1    │3     │4    │2          │
   * └───┴─────┴──────┴─────┴───────────┘
   * table4 (id: 4)
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
   *
   * If inserted, tables have the following number of rows:
   *
   * - table2: 3 rows
   * - table3: 5 rows
   * - table4: 8 rows
   */
  def createUnionTable(shouldInsertRows: Boolean = false, shouldCreateColumns: Boolean = true): Future[TableId] = {
  // format: on

    val createColumnName = createTextColumnJson("name")
    val createColumnColor = createMultilangTextColumnJson("color")
    val createColumnPrio = createNumberColumnJson("prio")

    for {
      glossLinkTableId <- createDefaultTable(name = "glossLink")
      createColumnGloss = createCardinalityLinkColumn(glossLinkTableId, "glossLevel", 0, 1)

      tableId2 <- sendRequest("POST", "/tables", createTableJson("table2")).map(_.getLong("id"))
      tableId3 <- sendRequest("POST", "/tables", createTableJson("table3")).map(_.getLong("id"))
      tableId4 <- sendRequest("POST", "/tables", createTableJson("table4")).map(_.getLong("id"))

      table2Columns = Columns(createColumnName, createColumnColor, createColumnPrio, createColumnGloss)
      table3Columns = Columns(createColumnName, createColumnGloss, createColumnColor, createColumnPrio)
      table4Columns = Columns(createColumnGloss, createColumnColor, createColumnPrio, createColumnName)

      // create columns with different ordering in each table
      _ <- sendRequest("POST", s"/tables/$tableId2/columns", table2Columns)
      _ <- sendRequest("POST", s"/tables/$tableId3/columns", table3Columns)
      _ <- sendRequest("POST", s"/tables/$tableId4/columns", table4Columns)

      _ <-
        if (shouldInsertRows) {
          insertTestDataIntoTables(tableId2, tableId3, tableId4)
        } else {
          Future.successful(())
        }

      payload = Json.obj(
        "name" -> "union",
        "type" -> "union",
        "displayName" -> Json.obj("de" -> "Union Table"),
        "originTables" -> Json.arr(tableId2, tableId4, tableId3)
      )
      resultUnionTable <- sendRequest("POST", "/tables", payload)

      _ <-
        if (shouldCreateColumns) {
          addColumnsToUnionTable(
            resultUnionTable.getLong("id"),
            Seq(unionTableCol2, unionTableCol3, unionTableCol4, unionTableCol5)
          )
        } else {
          Future.successful(())
        }

    } yield resultUnionTable.getLong("id")
  }
}

@RunWith(classOf[VertxUnitRunner])
class CreateUnionTableTest extends TableauxTestBase with UnionTableTestHelper {

  @Test
  def createUnionTable_withOriginTables_ok(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createUnionTable(false, false)
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
            "kind" -> "origintable"
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

    for {
      tableId <- createUnionTable(false, false)

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
        // "originColumns" -> Json.arr(
        //   Json.obj("tableId" -> 2, "columnId" -> 1),
        //   Json.obj("tableId" -> 3, "columnId" -> 1),
        //   Json.obj("tableId" -> 4, "columnId" -> 4)
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
        "originColumns" -> Json.arr(
          Json.obj("tableId" -> 2, "columnId" -> 1),
          Json.obj("tableId" -> 3, "columnId" -> 1),
          Json.obj("tableId" -> 4, "columnId" -> 4)
        )
      )

      for {
        tableId <- createUnionTable(false, false)
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
        "originColumns" -> Json.arr(
          Json.obj("tableId" -> 2, "columnId" -> 1),
          Json.obj("tableId" -> 3, "columnId" -> 1),
          Json.obj("tableId" -> 4, "columnId" -> 4)
        )
      )

      for {
        tableId <- createUnionTable(false, false)
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

@RunWith(classOf[VertxUnitRunner])
class NotImplementedUnionTableTest extends TableauxTestBase with UnionTableTestHelper {

  val expectedException = TestCustomException(
    "com.campudus.tableaux.NotImplementedException: Operation not implemented for table of type union",
    "error.request.notimplemented",
    501
  )

  @Test
  def unionTable_notImplementedMethods_shouldFail(implicit c: TestContext): Unit = okTest {
    // not implemented endpoints in TableauxController
    val cellPayload = Json.obj("value" -> Json.obj("de" -> "foo", "en" -> "bar"))
    val permissionsPayload = Json.obj("value" -> Json.arr("role:admin"))
    val orderPayload = Json.obj("location" -> "end")
    val anyUuid = "7e3982f3-7328-45f9-b499-9424a20bf5ff"

    for {
      tableId <- createUnionTable()
      retrieveCell <- sendRequest("GET", s"/tables/$tableId/columns/1/rows/1").toException()
      postCell <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/1", cellPayload).toException()
      patchCell <- sendRequest("PATCH", s"/tables/$tableId/columns/1/rows/1", cellPayload).toException()
      deleteCell <- sendRequest("DELETE", s"/tables/$tableId/columns/1/rows/1").toException()
      retrieveAnnotationsTable <- sendRequest("GET", s"/tables/$tableId/annotations").toException()
      retrieveRow <- sendRequest("GET", s"/tables/$tableId/rows/1").toException()
      retrieveForeignRows <-
        sendRequest("GET", s"/tables/$tableId/columns/1/rows/1/foreignRows").toException()
      retrieveDependentRows <- sendRequest("GET", s"/tables/$tableId/rows/1/dependent").toException()
      retrieveCellAnnotations <-
        sendRequest("GET", s"/tables/$tableId/columns/1/rows/1/annotations").toException()
      retrieveCellHistory <- sendRequest("GET", s"/tables/$tableId/columns/1/rows/1/history").toException()
      retrieveColumnHistory <- sendRequest("GET", s"/tables/$tableId/columns/1/history").toException()
      retrieveRowHistory <- sendRequest("GET", s"/tables/$tableId/rows/1/history").toException()
      retrieveTableHistory <- sendRequest("GET", s"/tables/$tableId/history").toException()
      addRowPermissions <-
        sendRequest("PATCH", s"/tables/$tableId/rows/1/permissions", permissionsPayload).toException()
      deleteRowPermissions <- sendRequest("DELETE", s"/tables/$tableId/rows/1/permissions").toException()
      replaceRowPermissions <-
        sendRequest("PUT", s"/tables/$tableId/rows/1/permissions", permissionsPayload).toException()
      retrieveCellHistory <-
        sendRequest("GET", s"/tables/$tableId/columns/1/rows/1/history").toException()
      createRow <- sendRequest("POST", s"/tables/$tableId/rows", Json.obj()).toException()
      duplicateRow <- sendRequest("POST", s"/tables/$tableId/rows/1/duplicate").toException()
      updateRowAnnotations <-
        sendRequest("PATCH", s"/tables/$tableId/rows/1/annotations", Json.obj()).toException()
      updateRowsAnnotations <-
        sendRequest("PATCH", s"/tables/$tableId/rows/annotations", Json.obj()).toException()
      changeLinkOrder <-
        sendRequest("PUT", s"/tables/$tableId/columns/4/rows/1/link/1/order", orderPayload).toException()
      deleteCellAnnotation <-
        sendRequest("DELETE", s"/tables/$tableId/columns/1/rows/1/annotations/$anyUuid").toException()
      deleteRow <- sendRequest("DELETE", s"/tables/$tableId/rows/1").toException()
      deleteAttachment <-
        sendRequest("DELETE", s"/tables/$tableId/columns/1/rows/1/attachment/$anyUuid").toException()
      deleteLink <- sendRequest("DELETE", s"/tables/$tableId/columns/1/rows/1/link/1").toException()
    } yield {
      assertEquals(expectedException, retrieveCell)
      assertEquals(expectedException, postCell)
      assertEquals(expectedException, patchCell)
      assertEquals(expectedException, deleteCell)
      assertEquals(expectedException, retrieveAnnotationsTable)
      assertEquals(expectedException, retrieveRow)
      assertEquals(expectedException, retrieveForeignRows)
      assertEquals(expectedException, retrieveDependentRows)
      assertEquals(expectedException, retrieveCellAnnotations)
      assertEquals(expectedException, retrieveCellHistory)
      assertEquals(expectedException, retrieveColumnHistory)
      assertEquals(expectedException, retrieveRowHistory)
      assertEquals(expectedException, retrieveTableHistory)
      assertEquals(expectedException, addRowPermissions)
      assertEquals(expectedException, deleteRowPermissions)
      assertEquals(expectedException, replaceRowPermissions)
      assertEquals(expectedException, retrieveCellHistory)
      assertEquals(expectedException, createRow)
      assertEquals(expectedException, duplicateRow)
      assertEquals(expectedException, updateRowAnnotations)
      assertEquals(expectedException, updateRowsAnnotations)
      assertEquals(expectedException, changeLinkOrder)
      assertEquals(expectedException, deleteCellAnnotation)
      assertEquals(expectedException, deleteRow)
      assertEquals(expectedException, deleteAttachment)
      assertEquals(expectedException, deleteLink)
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class RetrieveRowsUnionTableTest extends TableauxTestBase with UnionTableTestHelper {

  // EPs to implement for union tables

  // - retrieveRowsOfColumn <- sendRequest("GET", s"/tables/$tableId/columns/1/rows")
  // - retrieveRowsOfFirstColumn <- sendRequest("GET", s"/tables/$tableId/columns/1/first")
  // - retrieveRows <- sendRequest("GET", s"/tables/$tableId/rows")

  /**
    * Tables have the following number of rows:
    *
    *   - table2: 3 rows
    *   - table3: 5 rows
    *   - table4: 8 rows
    *
    * With union table ordering: table2, table4, table3 we get:
    *
    * request1: offset: 0, limit: 7, totalSize: 16 -> response has 7 rows
    *
    *   - table2: 3 rows
    *   - table3: 0 rows
    *   - table4: 4 rows
    *
    * request2: offset: 7, limit: 7, totalSize: 16 -> response has 7 rows
    *
    *   - table2: 0 rows
    *   - table3: 6 rows
    *   - table4: 1 rows
    *
    * request3: offset: 14, limit: 7, totalSize: 16 -> response has 2 rows
    *
    *   - table2: 0 rows
    *   - table3: 2 rows
    *   - table4: 0 rows
    */
  @Test
  def unionTable_retrieveRows_ok(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createUnionTable(true)
      retrieveRowsOfColumn <- sendRequest("GET", s"/tables/$tableId/columns/1/rows").toException()
      retrieveRowsOfFirstColumn <- sendRequest("GET", s"/tables/$tableId/columns/1/first").toException()

      retrieveAllUnionTableRows <- sendRequest("GET", s"/tables/$tableId/rows")
      retrieveTable2Rows <- sendRequest("GET", s"/tables/2/rows")
      retrieveTable3Rows <- sendRequest("GET", s"/tables/3/rows")
      retrieveTable4Rows <- sendRequest("GET", s"/tables/4/rows")

      retrieveFirstTwoRows <- sendRequest("GET", s"/tables/$tableId/rows?offset=0&limit=2")

      retrieveFirstSevenRows <- sendRequest("GET", s"/tables/$tableId/rows?offset=0&limit=7")
      retrieveSecondSevenRows <- sendRequest("GET", s"/tables/$tableId/rows?offset=7&limit=7")
      retrieveLastTwoRows <- sendRequest("GET", s"/tables/$tableId/rows?offset=14&limit=7")
    } yield {
      assertEquals(3, retrieveTable2Rows.getJsonArray("rows").size())
      assertEquals(5, retrieveTable3Rows.getJsonArray("rows").size())
      assertEquals(8, retrieveTable4Rows.getJsonArray("rows").size())

      assertEquals(16, retrieveAllUnionTableRows.getJsonArray("rows").size())
      assertJSONEquals(
        Json.obj("offset" -> null, "limit" -> null, "totalSize" -> 16),
        retrieveAllUnionTableRows.getJsonObject("page")
      )

      assertEquals(2, retrieveFirstTwoRows.getJsonArray("rows").size())
      assertJSONEquals(
        Json.obj("offset" -> 0, "limit" -> 2, "totalSize" -> 16),
        retrieveFirstTwoRows.getJsonObject("page")
      )

      assertEquals(7, retrieveFirstSevenRows.getJsonArray("rows").size())
      assertJSONEquals(
        Json.obj("offset" -> 0, "limit" -> 7, "totalSize" -> 16),
        retrieveFirstSevenRows.getJsonObject("page")
      )

      assertEquals(7, retrieveSecondSevenRows.getJsonArray("rows").size())
      assertJSONEquals(
        Json.obj("offset" -> 7, "limit" -> 7, "totalSize" -> 16),
        retrieveSecondSevenRows.getJsonObject("page")
      )

      assertEquals(2, retrieveLastTwoRows.getJsonArray("rows").size())
      assertJSONEquals(
        Json.obj("offset" -> 14, "limit" -> 7, "totalSize" -> 16),
        retrieveLastTwoRows.getJsonObject("page")
      )
    }
  }

  @Test
  def unionTable_retrieveAllRows_haveColumnOrderingOfUnionTable(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createUnionTable(true)
      retrieveAllUnionTableRows <- sendRequest("GET", s"/tables/$tableId/rows")

      retrieveTable4Rows <- sendRequest("GET", s"/tables/4/rows")
      retrieveTable4Columns <- sendRequest("GET", s"/tables/4/columns")
    } yield {
      val rows = retrieveAllUnionTableRows.getJsonArray("rows")

      val concatTableIds =
        rows.asScala.map(row =>
          row.asInstanceOf[JsonObject].getInteger("tableId")
        ).mkString(",")

      assertEquals("2,2,2,4,4,4,4,4,4,4,4,3,3,3,3,3", concatTableIds)

      val concatStringColumn1 =
        rows.asScala.map(row =>
          row.asInstanceOf[JsonObject].getJsonArray("values").getJsonObject(0).getString("de")
        ).mkString(",")

      assertEquals(
        List(
          List.fill(3)("table2_de").mkString(","),
          List.fill(8)("table4_de").mkString(","),
          List.fill(5)("table3_de").mkString(",")
        ).mkString(","),
        concatStringColumn1
      )

      val concatStringColumn2 =
        rows.asScala.map(row =>
          row.asInstanceOf[JsonObject].getJsonArray("values").getString(1)
        ).mkString(",")

      assertEquals(
        "color1,color2,color3,color4,color5,color6,color7,color8,color9,color10,color11,color12,color13,color14,color15,color16",
        concatStringColumn2
      )

      val concatStringColumn3 =
        rows.asScala.map(row =>
          row.asInstanceOf[JsonObject].getJsonArray("values").getJsonObject(2).getString("de")
        ).mkString(",")

      assertEquals(
        "Rot,Blau,Grün,Rot,Blau,Grün,Gelb,Schwarz,Weiß,Rosa,Lila,Rot,Blau,Grün,Gelb,Schwarz",
        concatStringColumn3
      )
      val concatStringColumn4 =
        rows.asScala.map(row =>
          row.asInstanceOf[JsonObject].getJsonArray("values").getInteger(3)
        ).mkString(",")

      assertEquals("1,2,3,1,2,3,4,5,6,7,8,1,2,3,4,5", concatStringColumn4)

      val concatStringColumn5 =
        rows.asScala.map(row =>
          row.asInstanceOf[JsonObject].getJsonArray("values").getJsonArray(4)
            .asScala.map(link => link.asInstanceOf[JsonObject].getInteger("id")).mkString(",")
        ).mkString(",")

      assertEquals("1,2,1,1,,2,,2,,1,2,1,,1,,1", concatStringColumn5)

    }
  }
}
