package com.campudus.tableaux.api.structure

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.Cardinality
import com.campudus.tableaux.database.domain.Constraint
import com.campudus.tableaux.database.model.TableauxModel.ColumnId
import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.testtools.RequestCreation._
import com.campudus.tableaux.testtools.TableauxTestBase
import com.campudus.tableaux.testtools.TestCustomException

import io.vertx.core.json._
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
    "languageType" -> "language",
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

  def fetchColumns(tableId: TableId) =
    sendRequest("GET", s"/tables/$tableId/columns").map(_.getJsonArray("columns")).map(_.asScala.collect({
      case obj: JsonObject => obj
    }).toSeq)

  def fetchColumn(tableId: TableId, columnId: ColumnId) = sendRequest("GET", s"/tables/$tableId/columns/$columnId")

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
    exceptionTest("error.arguments") {
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
    okTest {
      val expectedException = TestCustomException(
        "com.campudus.tableaux.NotFoundInDatabaseException: error.database.notfound.table: "
          + "originTables with ids [999] not found",
        "NOT FOUND",
        404
      )

      for {
        exception <- sendRequest(
          "POST",
          "/tables",
          Json.obj(
            "name" -> "union",
            "type" -> "union",
            "displayName" -> Json.obj("de" -> "Union Table"),
            "originTables" -> Json.arr(999)
          )
        ).toException()
      } yield {
        assertEquals(expectedException, exception)
      }
    }

  @Test
  def createUnionTable_onAnotherUnionTables_shouldFail(implicit c: TestContext): Unit =
    exceptionTest("unprocessable.entity") {
      for {
        tableId <- createUnionTable(false, false)

        _ <- sendRequest(
          "POST",
          "/tables",
          Json.obj(
            "name" -> "second_union_table",
            "type" -> "union",
            "originTables" -> Json.arr(tableId)
          )
        )
      } yield ()
    }

  @Test
  def createGenericTable_withOriginTables_shouldFail(implicit c: TestContext): Unit =
    exceptionTest("error.arguments") {
      for {
        _ <- sendRequest(
          "POST",
          "/tables",
          Json.obj(
            "name" -> "second_union_table",
            "type" -> "generic",
            "originTables" -> Json.arr(1, 2, 3)
          )
        )
      } yield ()
    }

  @Test
  def createUnionTable_addColumnWithInvalidOriginTableColumn_shouldFail(implicit c: TestContext): Unit =
    okTest {
      val expectedException = TestCustomException(
        "com.campudus.tableaux.InvalidJsonException: "
          + "At least one CreateColumn contains originColumns for tables which are not defined in "
          + "originTables of the union table. Invalid tableIds: (3)",
        "error.json.unionTable",
        400
      )

      val createColumnName = createTextColumnJson("name")
      val createColumnColor = createMultilangTextColumnJson("color")
      val createColumnPrio = createNumberColumnJson("prio")

      for {
        tableId1 <- sendRequest("POST", "/tables", createTableJson("table2")).map(_.getLong("id"))
        tableId2 <- sendRequest("POST", "/tables", createTableJson("table3")).map(_.getLong("id"))
        tableId3 <- sendRequest("POST", "/tables", createTableJson("table4")).map(_.getLong("id"))

        table1Columns = Columns(createColumnName, createColumnColor, createColumnPrio)
        table2Columns = Columns(createColumnName, createColumnColor, createColumnPrio)
        table3Columns = Columns(createColumnColor, createColumnPrio, createColumnName)

        // create columns with different ordering in each table
        _ <- sendRequest("POST", s"/tables/$tableId1/columns", table1Columns)
        _ <- sendRequest("POST", s"/tables/$tableId2/columns", table2Columns)
        _ <- sendRequest("POST", s"/tables/$tableId3/columns", table3Columns)

        payload = Json.obj(
          "name" -> "union",
          "type" -> "union",
          "displayName" -> Json.obj("de" -> "Union Table"),
          "originTables" -> Json.arr(tableId1, tableId2)
        )
        unionTableId <- sendRequest("POST", "/tables", payload).map(_.getLong("id"))

        unionTableColPayload = Json.obj(
          "columns" -> Json.arr(
            Json.obj(
              "name" -> "name",
              "kind" -> "text",
              "ordering" -> 1,
              "originColumns" -> Json.arr(
                Json.obj("tableId" -> 1, "columnId" -> 1),
                Json.obj("tableId" -> 2, "columnId" -> 1),
                Json.obj("tableId" -> 3, "columnId" -> 3) // tableId4 is not defined in originTables
              )
            )
          )
        )

        exception <- sendRequest("POST", s"/tables/$unionTableId/columns", unionTableColPayload).toException()

      } yield {
        assertEquals(expectedException, exception)
      }
    }
}

@RunWith(classOf[VertxUnitRunner])
class UpdateUnionTableTest extends TableauxTestBase with UnionTableTestHelper {

  @Test
  def updateUnionTable_addColumnsToUnionTable_ok(implicit c: TestContext): Unit = okTest {

    def omitFields = omit(Seq("originColumns"), _)

    for {
      tableId <- createUnionTable(false, false)

      columns <- sendRequest(
        "POST",
        s"/tables/$tableId/columns",
        Json.obj("columns" -> Json.arr(unionTableCol2, unionTableCol3, unionTableCol4, unionTableCol5))
      )

      unionTable <- fetchTable(tableId)
      unionColumns <- fetchColumns(tableId)
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

      assertJSONEquals(omitFields(unionTableCol1), unionColumns(0))
      assertJSONEquals(omitFields(unionTableCol2), unionColumns(1))
      assertJSONEquals(omitFields(unionTableCol3), unionColumns(2))
      assertJSONEquals(omitFields(unionTableCol4), unionColumns(3))
      assertJSONEquals(omitFields(unionTableCol5), unionColumns(4))
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
    okTest {
      val expectedException = TestCustomException(
        "com.campudus.tableaux.InvalidJsonException: "
          + "CreateColumn 'name_two' has no valid field originColumns",
        "error.json.unionTable",
        400
      )

      val unionTableColWithoutOriginColumns = Json.obj(
        "name" -> "name_two",
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
        exception <- sendRequest(
          "POST",
          s"/tables/$tableId/columns",
          Json.obj("columns" -> Json.arr(unionTableColWithoutOriginColumns))
        ).toException()
      } yield {
        assertEquals(expectedException, exception)
      }
    }

  @Test
  def updateUnionTable_addInvalidColumnsToUnionTable_shouldFail(implicit c: TestContext): Unit = okTest {

    val expectedException = TestCustomException(
      "com.campudus.tableaux.InvalidJsonException: "
        + "At least one CreateColumn contains originColumns for tables which are not defined in originTables of the union table. Invalid tableIds: (77), "
        + "Column '1' in table '2' and CreateColumn 'name' have different values in field kind: text != numeric, "
        + "Column '2' in table '2' and CreateColumn 'color' have different values in field languageType: language != neutral, "
        + "Column '99' not found in table '2', "
        + "Column '1' in table '3' and CreateColumn 'name' have different values in field kind: text != numeric, "
        + "Column '3' in table '3' and CreateColumn 'color' have different values in field languageType: language != neutral, "
        + "Column '4' in table '4' and CreateColumn 'name' have different values in field kind: text != numeric, "
        + "Column '2' in table '4' and CreateColumn 'color' have different values in field languageType: language != neutral, "
        + "Table '77' could not be checked, possibly it does not exist",
      "error.json.unionTable",
      400
    )

    val col2 = Json.obj(
      "name" -> "name",
      "kind" -> "numeric", // wrong kind, should be "text"
      "ordering" -> 1,
      "originColumns" -> Json.arr(
        Json.obj("tableId" -> 2, "columnId" -> 1),
        Json.obj("tableId" -> 3, "columnId" -> 1),
        Json.obj("tableId" -> 4, "columnId" -> 4)
      )
    )

    val col3 = Json.obj(
      "name" -> "color",
      "kind" -> "text",
      "languageType" -> "neutral", // wrong languageType, should be "language"
      "originColumns" -> Json.arr(
        Json.obj("tableId" -> 2, "columnId" -> 2),
        Json.obj("tableId" -> 3, "columnId" -> 3),
        Json.obj("tableId" -> 4, "columnId" -> 2)
      )
    )

    val col4 = Json.obj(
      "name" -> "prio",
      "kind" -> "numeric",
      "originColumns" -> Json.arr(
        // wrong tableId, all tableIds from union table originTables must exist, be valid and of type generic
        Json.obj("tableId" -> 77, "columnId" -> 3),
        Json.obj("tableId" -> 3, "columnId" -> 4),
        Json.obj("tableId" -> 4, "columnId" -> 3)
      )
    )

    val col5 = Json.obj(
      "name" -> "glossLevel",
      "kind" -> "link",
      // wrong columnId, all columnIds from the origin tables must exist
      "originColumns" -> Json.arr(
        Json.obj("tableId" -> 2, "columnId" -> 99), // invalid columnId
        Json.obj("tableId" -> 3, "columnId" -> 2),
        Json.obj("tableId" -> 4, "columnId" -> 1)
      )
    )

    for {
      tableId <- createUnionTable(false, false)

      columnsException <- sendRequest(
        "POST",
        s"/tables/$tableId/columns",
        Json.obj("columns" -> Json.arr(col2, col3, col4, col5))
      ).toException()
    } yield {
      assertEquals(expectedException, columnsException)
    }
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

  @Test
  def deleteUnionTable_deleteColumnInOriginTable_ok(implicit c: TestContext): Unit =
    okTest {
      for {
        tableId <- createUnionTable(true)
        retrieveToBuildUpColumnCache <- sendRequest("GET", s"/tables/$tableId/rows")
        _ <- sendRequest("DELETE", s"/tables/4/columns/4")

        retrieveAllUnionTableRows <- sendRequest("GET", s"/tables/$tableId/rows").map(_.getJsonArray("rows"))
      } yield {
        assertEquals(16, retrieveAllUnionTableRows.size())
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
      postCell <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/1", cellPayload).toException()
      patchCell <- sendRequest("PATCH", s"/tables/$tableId/columns/1/rows/1", cellPayload).toException()
      deleteCell <- sendRequest("DELETE", s"/tables/$tableId/columns/1/rows/1").toException()
      retrieveAnnotationsTable <- sendRequest("GET", s"/tables/$tableId/annotations").toException()
      retrieveForeignRows <-
        sendRequest("GET", s"/tables/$tableId/columns/1/rows/1/foreignRows").toException()
      retrieveDependentRows <- sendRequest("GET", s"/tables/$tableId/rows/1/dependent").toException()
      addRowPermissions <-
        sendRequest("PATCH", s"/tables/$tableId/rows/1/permissions", permissionsPayload).toException()
      deleteRowPermissions <- sendRequest("DELETE", s"/tables/$tableId/rows/1/permissions").toException()
      replaceRowPermissions <-
        sendRequest("PUT", s"/tables/$tableId/rows/1/permissions", permissionsPayload).toException()
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
      assertEquals(expectedException, postCell)
      assertEquals(expectedException, patchCell)
      assertEquals(expectedException, deleteCell)
      assertEquals(expectedException, retrieveAnnotationsTable)
      assertEquals(expectedException, retrieveForeignRows)
      assertEquals(expectedException, retrieveDependentRows)
      assertEquals(expectedException, addRowPermissions)
      assertEquals(expectedException, deleteRowPermissions)
      assertEquals(expectedException, replaceRowPermissions)
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

  def getRowValuesAt(rows: JsonArray, index: Int): Seq[Any] = {
    rows.asScala.map(row =>
      row.asInstanceOf[JsonObject].getJsonArray("values").getValue(index)
    ).toSeq
  }

  def getRowValueCount(rows: JsonArray): Int =
    rows.asScala.headOption.map(row =>
      row.asInstanceOf[JsonObject].getJsonArray("values").size()
    ).getOrElse(0)

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
  def unionTable_retrieveRowsWithColumnFilter_ok(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createUnionTable(true)

      retrieveRowFilteredByColumns <- sendRequest("GET", s"/tables/$tableId/rows/2000001?columnIds=1,3")
      retrieveRowsFilteredByColumns <-
        sendRequest("GET", s"/tables/$tableId/rows?columnNames=originTable,color").map(_.getJsonArray("rows"))
    } yield {
      val expectedValues = """[{"de":"table2_de","en":"table2_en"},{"de":"Rot","en":"Red"}]"""
      val rowById = retrieveRowFilteredByColumns.getJsonArray("values")
      val rowsByHead = retrieveRowsFilteredByColumns.asScala.headOption
        .map(row => row.asInstanceOf[JsonObject].getJsonArray("values"))
        .getOrElse(null)

      assertEquals(2, rowById.size())
      assertJSONEquals(expectedValues, rowById)

      assertEquals(2, rowsByHead.size())
      assertJSONEquals(expectedValues, rowsByHead)
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

      val concatStringColumn1 = getRowValuesAt(rows, 0)
        .map(_.asInstanceOf[JsonObject].getString("de")).mkString(",")

      assertEquals(
        List(
          List.fill(3)("table2_de").mkString(","),
          List.fill(8)("table4_de").mkString(","),
          List.fill(5)("table3_de").mkString(",")
        ).mkString(","),
        concatStringColumn1
      )

      val concatStringColumn2 = getRowValuesAt(rows, 1).map(_.toString).mkString(",")

      assertEquals(
        "color1,color2,color3,color4,color5,color6,color7,color8,color9,color10,color11,color12,color13,color14,color15,color16",
        concatStringColumn2
      )

      val concatStringColumn3 = getRowValuesAt(rows, 2)
        .map(_.asInstanceOf[JsonObject].getString("de")).mkString(",")

      assertEquals(
        "Rot,Blau,Grün,Rot,Blau,Grün,Gelb,Schwarz,Weiß,Rosa,Lila,Rot,Blau,Grün,Gelb,Schwarz",
        concatStringColumn3
      )
      val concatStringColumn4 = getRowValuesAt(rows, 3).map(_.toString).mkString(",")

      assertEquals("1,2,3,1,2,3,4,5,6,7,8,1,2,3,4,5", concatStringColumn4)

      val concatStringColumn5 = getRowValuesAt(rows, 4)
        .map(v =>
          v.asInstanceOf[JsonArray].asScala.map(link => link.asInstanceOf[JsonObject].getInteger("id")).mkString(",")
        ).mkString(",")

      assertEquals("1,2,1,1,,2,,2,,1,2,1,,1,,1", concatStringColumn5)

    }
  }

  @Test
  def unionTablePlain_retrieveRows_ok(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createUnionTable(true, false)
      rowsResult <- sendRequest("GET", s"/tables/$tableId/rows")
      columnsResult <- sendRequest("GET", s"/tables/$tableId/columns")
    } yield {
      assertEquals(16, rowsResult.getJsonArray("rows").size())

      val rows = rowsResult.getJsonArray("rows")
      val columnsCount = columnsResult.getJsonArray("columns").size()
      assertEquals(1, columnsCount)
      assertEquals(1, getRowValueCount(rows))
    }
  }

  @Test
  def unionTable_retrieveRowsWithConcatColumn_ok(implicit c: TestContext): Unit = okTest {
    val expectedConcatValue = Json.arr(
      Json.obj("de" -> "table2_de", "en" -> "table2_en"),
      "color1"
    )

    for {
      tableId <- createUnionTable(true)
      // change column 2 to be second identifier column
      _ <- sendRequest(
        "PATCH",
        s"/tables/$tableId/columns/2",
        Json.obj("identifier" -> true)
      )

      retrieveRows <- sendRequest("GET", s"/tables/$tableId/rows").map(_.getJsonArray("rows"))
      retrieveRow <- sendRequest("GET", s"/tables/$tableId/rows/2000001")
    } yield {
      val concatValueFirstRow1 = retrieveRows.asScala.headOption
        .map(row => row.asInstanceOf[JsonObject].getJsonArray("values").getJsonArray(0))
        .getOrElse(null)
      val concatValueFirstRow2 = retrieveRow.getJsonArray("values").getJsonArray(0)

      assertJSONEquals(expectedConcatValue, concatValueFirstRow1)
      assertJSONEquals(expectedConcatValue, concatValueFirstRow2)
    }
  }

  @Test
  def unionTablePlain_retrieveRowsOfFirstColumn_ok(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createUnionTable(true, true)
      rowsResult <- sendRequest("GET", s"/tables/$tableId/columns/first/rows")
      rowsResultPaginated <- sendRequest("GET", s"/tables/$tableId/columns/first/rows?offset=10&limit=2")
    } yield {
      val rows = rowsResult.getJsonArray("rows")

      assertEquals(16, rows.size())
      assertEquals(1, getRowValueCount(rows))
      val valueTable2 = Json.obj("de" -> "table2_de", "en" -> "table2_en")
      val valueTable3 = Json.obj("de" -> "table3_de", "en" -> "table3_en")
      val valueTable4 = Json.obj("de" -> "table4_de", "en" -> "table4_en")

      val expectedValues =
        Seq.fill(3)(valueTable2) ++
          Seq.fill(8)(valueTable4) ++
          Seq.fill(5)(valueTable3)

      assertEquals(expectedValues, getRowValuesAt(rows, 0))

      // paginated
      val rowsPaginated = rowsResultPaginated.getJsonArray("rows")
      assertEquals(1, getRowValueCount(rowsPaginated))
      assertEquals(2, rowsPaginated.size())

      val expectedPaginatedValues = Seq(valueTable4, valueTable3) // one row from table4 and one from table3
      assertEquals(expectedPaginatedValues, getRowValuesAt(rowsPaginated, 0))
    }
  }

  @Test
  def unionTablePlain_retrieveRowsOfSpecificColumn_ok(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createUnionTable(true, true)
      rowsResultColumn1 <- sendRequest("GET", s"/tables/$tableId/columns/1/rows")
      rowsResultColumn2 <- sendRequest("GET", s"/tables/$tableId/columns/2/rows")
      rowsResultPaginatedColumn1 <- sendRequest("GET", s"/tables/$tableId/columns/1/rows?offset=10&limit=2")
      rowsResultPaginatedColumn2 <- sendRequest("GET", s"/tables/$tableId/columns/2/rows?offset=10&limit=2")
    } yield {
      val rowsColumn1 = rowsResultColumn1.getJsonArray("rows")
      val rowsColumn2 = rowsResultColumn2.getJsonArray("rows")

      assertEquals(16, rowsColumn1.size())
      assertEquals(16, rowsColumn2.size())
      assertEquals(1, getRowValueCount(rowsColumn1))
      assertEquals(1, getRowValueCount(rowsColumn2))

      val value1Table2 = Json.obj("de" -> "table2_de", "en" -> "table2_en")
      val value1Table3 = Json.obj("de" -> "table3_de", "en" -> "table3_en")
      val value1Table4 = Json.obj("de" -> "table4_de", "en" -> "table4_en")

      val expectedValues1 =
        Seq.fill(3)(value1Table2) ++
          Seq.fill(8)(value1Table4) ++
          Seq.fill(5)(value1Table3)

      assertEquals(expectedValues1, getRowValuesAt(rowsColumn1, 0))

      println(s"###LOG###: rowsColumn1: ${rowsColumn1}")
      println(s"###LOG###: rowsColumn2: ${rowsColumn2}")

      val expectedValues2 =
        Seq("color1", "color2", "color3", "color4", "color5", "color6", "color7", "color8") ++
          Seq("color9", "color10", "color11", "color12", "color13", "color14", "color15", "color16")

      assertEquals(expectedValues2, getRowValuesAt(rowsColumn2, 0))

      // paginated
      val rowsPaginatedColumn1 = rowsResultPaginatedColumn1.getJsonArray("rows")
      val rowsPaginatedColumn2 = rowsResultPaginatedColumn2.getJsonArray("rows")

      assertEquals(2, rowsPaginatedColumn1.size())
      assertEquals(2, rowsPaginatedColumn2.size())
      assertEquals(1, getRowValueCount(rowsPaginatedColumn1))
      assertEquals(1, getRowValueCount(rowsPaginatedColumn2))
      val expectedPaginatedValues1 = Seq(value1Table4, value1Table3) // one row from table4 and one from table3
      assertEquals(expectedPaginatedValues1, getRowValuesAt(rowsPaginatedColumn1, 0))

      val expectedPaginatedValues2 = Seq("color11", "color12") // one row from table4 and one from table3
      assertEquals(expectedPaginatedValues2, getRowValuesAt(rowsPaginatedColumn2, 0))
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class RetrieveColumnsUnionTableTest extends TableauxTestBase with UnionTableTestHelper {

  @Test
  def unionTable_retrieveColumns_ok(implicit c: TestContext): Unit = okTest {

    def omitFields = omit(Seq("permission", "status"), _)

    for {
      tableId <- createUnionTable()
      columns <- fetchColumns(tableId)

      unionColumn2_1 <- fetchColumn(2, 1)
      unionColumn3_1 <- fetchColumn(3, 1)
      unionColumn4_4 <- fetchColumn(4, 4)

      unionColumn2_2 <- fetchColumn(2, 2)
      unionColumn3_3 <- fetchColumn(3, 3)
      unionColumn4_2 <- fetchColumn(4, 2)

      unionColumn2_3 <- fetchColumn(2, 3)
      unionColumn3_4 <- fetchColumn(3, 4)
      unionColumn4_3 <- fetchColumn(4, 3)

      unionColumn2_4 <- fetchColumn(2, 4)
      unionColumn3_2 <- fetchColumn(3, 2)
      unionColumn4_1 <- fetchColumn(4, 1)

    } yield {
      assertFalse(columns(0).containsKey("originColumns"))

      val unionTableCol2withOriginColumns = unionTableCol2.mergeIn(
        Json.obj("originColumns" -> Json.arr(
          Json.obj("tableId" -> 2, "column" -> omitFields(unionColumn2_1)),
          Json.obj("tableId" -> 3, "column" -> omitFields(unionColumn3_1)),
          Json.obj("tableId" -> 4, "column" -> omitFields(unionColumn4_4))
        ))
      )

      val unionTableCol3withOriginColumns = unionTableCol3.mergeIn(
        Json.obj("originColumns" -> Json.arr(
          Json.obj("tableId" -> 2, "column" -> omitFields(unionColumn2_2)),
          Json.obj("tableId" -> 3, "column" -> omitFields(unionColumn3_3)),
          Json.obj("tableId" -> 4, "column" -> omitFields(unionColumn4_2))
        ))
      )

      val unionTableCol4withOriginColumns = unionTableCol4.mergeIn(
        Json.obj("originColumns" -> Json.arr(
          Json.obj("tableId" -> 2, "column" -> omitFields(unionColumn2_3)),
          Json.obj("tableId" -> 3, "column" -> omitFields(unionColumn3_4)),
          Json.obj("tableId" -> 4, "column" -> omitFields(unionColumn4_3))
        ))
      )

      val unionTableCol5withOriginColumns = unionTableCol5.mergeIn(
        Json.obj("originColumns" -> Json.arr(
          Json.obj("tableId" -> 2, "column" -> omitFields(unionColumn2_4)),
          Json.obj("tableId" -> 3, "column" -> omitFields(unionColumn3_2)),
          Json.obj("tableId" -> 4, "column" -> omitFields(unionColumn4_1))
        ))
      )

      assertJSONEquals(unionTableCol2withOriginColumns, columns(1))
      assertJSONEquals(unionTableCol3withOriginColumns, columns(2))
      assertJSONEquals(unionTableCol4withOriginColumns, columns(3))
      assertJSONEquals(unionTableCol5withOriginColumns, columns(4))
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class RetrieveRowUnionTableTest extends TableauxTestBase with UnionTableTestHelper {

  @Test
  def unionTable_retrieveRow_ok(implicit c: TestContext): Unit = okTest {
    val valuesTable2Row1 = Json.arr(
      Json.obj("de" -> "table2_de", "en" -> "table2_en"),
      "color1",
      Json.obj("de" -> "Rot", "en" -> "Red"),
      1,
      Json.arr(Json.obj("id" -> 1, "value" -> "table1row1"))
    )

    val valuesTable4Row8 = Json.arr(
      Json.obj("de" -> "table4_de", "en" -> "table4_en"),
      "color11",
      Json.obj("de" -> "Lila", "en" -> "Purple"),
      8,
      Json.arr(Json.obj("id" -> 2, "value" -> "table1row2"))
    )

    for {
      tableId <- createUnionTable(true)
      resultRowTable2Row1 <- sendRequest("GET", s"/tables/$tableId/rows/2000001")
      resultRowTable4Row8 <- sendRequest("GET", s"/tables/$tableId/rows/4000008")

    } yield {
      assertEquals(2000001, resultRowTable2Row1.getInteger("id"))
      assertEquals(2, resultRowTable2Row1.getInteger("tableId"))
      assertJSONEquals(valuesTable2Row1, resultRowTable2Row1.getJsonArray("values"))

      assertEquals(4000008, resultRowTable4Row8.getInteger("id"))
      assertEquals(4, resultRowTable4Row8.getInteger("tableId"))
      assertJSONEquals(valuesTable4Row8, resultRowTable4Row8.getJsonArray("values"))
    }
  }

  @Test
  def unionTable_retrieveRowWithTwoColumnsToSameOriginColumnInOneTable_ok(implicit c: TestContext): Unit = okTest {
    val valuesTable2Row1 = Json.arr(
      Json.obj("de" -> "table2_de", "en" -> "table2_en"),
      "color1",
      Json.obj("de" -> "Rot", "en" -> "Red"),
      Json.obj("de" -> "Rot", "en" -> "Red"),
      1,
      Json.arr(Json.obj("id" -> 1, "value" -> "table1row1"))
    )

    val secondColumnToOriginColumnColor =
      Json.obj(
        "name" -> "color2",
        "kind" -> "text",
        "ordering" -> 2,
        "languageType" -> "language",
        "displayName" -> Json.obj("de" -> "Name der Farbe 2", "en" -> "Color Name 2"),
        "description" -> Json.obj("de" -> "Name der Farbe 2", "en" -> "Name of the color 2"),
        "originColumns" -> Json.arr(
          Json.obj("tableId" -> 2, "columnId" -> 2),
          Json.obj("tableId" -> 3, "columnId" -> 3),
          Json.obj("tableId" -> 4, "columnId" -> 2)
        )
      )

    for {
      tableId <- createUnionTable(true)
      // add another column that maps to the same origin columns as column 3
      columns <- sendRequest(
        "POST",
        s"/tables/$tableId/columns",
        Json.obj("columns" -> Json.arr(secondColumnToOriginColumnColor))
      )
      resultRowTable2Row1 <- sendRequest("GET", s"/tables/$tableId/rows/2000001")

    } yield {
      assertEquals(2000001, resultRowTable2Row1.getInteger("id"))
      assertEquals(2, resultRowTable2Row1.getInteger("tableId"))
      assertJSONEquals(valuesTable2Row1, resultRowTable2Row1.getJsonArray("values"))
    }
  }

  @Test
  def unionTable_retrieveRowThatDoesNotExist_shouldFail(implicit c: TestContext): Unit =
    exceptionTest("NOT FOUND") {
      for {
        tableId <- createUnionTable(true)
        _ <- sendRequest("GET", s"/tables/$tableId/rows/4000009")

      } yield ()
    }
}

@RunWith(classOf[VertxUnitRunner])
class RetrieveAnnotationsUnionTableTest extends TableauxTestBase with UnionTableTestHelper {

  @Test
  def unionTable_retrieveAnnotations_ok(implicit c: TestContext): Unit = okTest {

    val expectedAnnotations = Json.arr(
      null, // this is the concat column
      null, // this is the originTable column
      Json.arr(Json.obj("type" -> "flag", "value" -> "check-me")),
      Json.arr(Json.obj("type" -> "flag", "value" -> "needs_translation", "langtags" -> Json.arr("de", "en"))),
      Json.arr(
        Json.obj("type" -> "info", "value" -> "this is a comment"),
        Json.obj("type" -> "info", "value" -> "this is another comment")
      ),
      Json.arr(
        Json.obj("type" -> "error", "value" -> null),
        Json.obj("type" -> "flag", "value" -> "important")
      )
    )
    val checkMeFlag = """{"type": "flag", "value": "check-me"}"""
    val importantFlag = """{"type": "flag", "value": "important"}"""
    val errorAnnotation = """{"type": "error"}"""
    val commentAnnotation = (msg: String) => s"""{"type": "info", "value": "${msg}"}"""
    val needsTranslationFlag = """{"langtags": ["de", "en"], "type": "flag", "value": "needs_translation"}"""

    for {
      tableId <- createUnionTable(true)
      // change column 2 to be second identifier column
      _ <- sendRequest(
        "PATCH",
        s"/tables/$tableId/columns/2",
        Json.obj("identifier" -> true)
      )

      // add some annotations to an origin table 4 (because table 4 has very different column ordering)
      _ <- sendRequest("POST", s"/tables/4/columns/1/rows/1/annotations", errorAnnotation)
      _ <- sendRequest("POST", s"/tables/4/columns/1/rows/1/annotations", importantFlag)
      _ <- sendRequest("POST", s"/tables/4/columns/2/rows/1/annotations", needsTranslationFlag)
      _ <- sendRequest("POST", s"/tables/4/columns/3/rows/1/annotations", commentAnnotation("this is a comment"))
      _ <- sendRequest("POST", s"/tables/4/columns/3/rows/1/annotations", commentAnnotation("this is another comment"))
      _ <- sendRequest("POST", s"/tables/4/columns/4/rows/1/annotations", checkMeFlag)

      retrieveRows <- sendRequest("GET", s"/tables/$tableId/rows").map(_.getJsonArray("rows"))
      retrieveRow <- sendRequest("GET", s"/tables/$tableId/rows/4000001")
    } yield {
      val annotationsFirstRow1 = retrieveRows.asScala.toSeq.lift(3) // first row from table 4
        .map(row => row.asInstanceOf[JsonObject].getJsonArray("annotations"))
        .getOrElse(null)
      val annotationsFirstRow2 = retrieveRow.getJsonArray("annotations")

      assertJSONEquals(expectedAnnotations, annotationsFirstRow1)
      assertJSONEquals(expectedAnnotations, annotationsFirstRow2)
    }
  }

  @Test
  def unionTable_retrieveCellAnnotations_ok(implicit c: TestContext): Unit = okTest {

    val expectedAnnotations = Json.arr(
      Json.arr(
        Json.obj("type" -> "info", "value" -> "this is a comment"),
        Json.obj("type" -> "flag", "value" -> "check-me")
      )
    )

    val checkMeFlag = """{"type": "flag", "value": "check-me"}"""
    val commentAnnotation = (msg: String) => s"""{"type": "info", "value": "${msg}"}"""

    for {
      tableId <- createUnionTable(true)

      // add two annotations to an origin table 4 (because table 4 has very different column ordering)
      _ <- sendRequest("POST", s"/tables/4/columns/3/rows/1/annotations", commentAnnotation("this is a comment"))
      _ <- sendRequest("POST", s"/tables/4/columns/3/rows/1/annotations", checkMeFlag)

      annotations <-
        sendRequest("GET", s"/tables/$tableId/columns/4/rows/4000001/annotations").map(_.getJsonArray("annotations"))
    } yield {
      assertJSONEquals(expectedAnnotations, annotations)
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class RetrieveHistoryUnionTableTest extends TableauxTestBase with UnionTableTestHelper {

  @Test
  def unionTable_retrieveCellHistory_ok(implicit c: TestContext): Unit = okTest {
    val expectedHistory = Json.arr(
      Json.obj("revision" -> 1, "rowId" -> 4000001, "event" -> "row_created"),
      Json.obj("revision" -> 5, "rowId" -> 4000001, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("revision" -> 41, "rowId" -> 4000001, "event" -> "annotation_added", "columnId" -> 5)
    )
    val importantFlag = """{"type": "flag", "value": "important"}"""

    for {
      tableId <- createUnionTable(true)
      _ <- sendRequest("POST", s"/tables/4/columns/1/rows/1/annotations", importantFlag)
      cellHistory <- sendRequest("GET", s"/tables/$tableId/columns/5/rows/4000001/history").map(_.getJsonArray("rows"))
    } yield {
      assertJSONEquals(expectedHistory, cellHistory)
    }
  }

  @Test
  def unionTable_retrieveRowHistory_ok(implicit c: TestContext): Unit = okTest {
    val expectedHistory = Json.arr( // without values and revision, because order of history creation is not guaranteed
      Json.obj("rowId" -> 4000001, "event" -> "row_created"),
      Json.obj("rowId" -> 4000001, "event" -> "cell_changed", "columnId" -> 4),
      Json.obj("rowId" -> 4000001, "event" -> "cell_changed", "columnId" -> 2),
      Json.obj("rowId" -> 4000001, "event" -> "cell_changed", "columnId" -> 3),
      Json.obj("rowId" -> 4000001, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 4000001, "event" -> "annotation_added", "columnId" -> 5)
    )
    val importantFlag = """{"type": "flag", "value": "important"}"""

    for {
      tableId <- createUnionTable(true)
      _ <- sendRequest("POST", s"/tables/4/columns/1/rows/1/annotations", importantFlag)
      rowHistory <- sendRequest("GET", s"/tables/$tableId/rows/4000001/history").map(_.getJsonArray("rows"))
    } yield {
      assertJSONEquals(expectedHistory, rowHistory)
    }
  }

  @Test
  def unionTable_retrieveColumnHistory_ok(implicit c: TestContext): Unit = okTest {
    val expectedHistory = Json.arr( // without values and revision, because order of history creation is not guaranteed
      Json.obj("rowId" -> 2000001, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 2000002, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 2000003, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 4000001, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 4000002, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 4000003, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 4000004, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 4000005, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 4000006, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 4000007, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 4000008, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 4000001, "event" -> "annotation_added", "columnId" -> 5),
      Json.obj("rowId" -> 3000001, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 3000002, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 3000003, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 3000004, "event" -> "cell_changed", "columnId" -> 5),
      Json.obj("rowId" -> 3000005, "event" -> "cell_changed", "columnId" -> 5)
    )
    val importantFlag = """{"type": "flag", "value": "important"}"""

    for {
      tableId <- createUnionTable(true)
      _ <- sendRequest("POST", s"/tables/4/columns/1/rows/1/annotations", importantFlag)
      columnHistory <- sendRequest("GET", s"/tables/$tableId/columns/5/history").map(_.getJsonArray("rows"))
    } yield {
      assertJSONEquals(expectedHistory, columnHistory)
    }
  }

  @Test
  def unionTable_retrieveTableHistory_ok(implicit c: TestContext): Unit = okTest {
    val firstHistoryOfTable2 = Json.obj("revision" -> 1, "rowId" -> 2000001, "event" -> "row_created")
    val firstHistoryOfTable4 = Json.obj("revision" -> 1, "rowId" -> 4000001, "event" -> "row_created")
    val firstHistoryOfTable3 = Json.obj("revision" -> 1, "rowId" -> 3000001, "event" -> "row_created")

    val importantFlag = """{"type": "flag", "value": "important"}"""

    for {
      tableId <- createUnionTable(true)
      _ <- sendRequest("POST", s"/tables/4/columns/1/rows/1/annotations", importantFlag)
      tableHistory <- sendRequest("GET", s"/tables/$tableId/history").map(_.getJsonArray("rows"))
    } yield {
      assertEquals(81, tableHistory.size())
      assertJSONEquals(firstHistoryOfTable2, tableHistory.asScala.toSeq(0).asInstanceOf[JsonObject])
      assertJSONEquals(firstHistoryOfTable4, tableHistory.asScala.toSeq(15).asInstanceOf[JsonObject])
      assertJSONEquals(firstHistoryOfTable3, tableHistory.asScala.toSeq(56).asInstanceOf[JsonObject])
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class RetrieveCellUnionTableTest extends TableauxTestBase with UnionTableTestHelper {

  @Test
  def unionTable_retrieveCell_ok(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createUnionTable(true)
      cellValue1 <- sendRequest("GET", s"/tables/$tableId/columns/3/rows/4000001").map(_.getJsonObject("value"))
      cellValue2 <- sendRequest("GET", s"/tables/$tableId/columns/4/rows/4000001").map(_.getLong("value"))
    } yield {
      assertJSONEquals(Json.obj("de" -> "Rot", "en" -> "Red"), cellValue1)
      assertEquals(1L, cellValue2)
    }
  }

  @Test
  def unionTable_retrieveCellFromOriginTableColumn_ok(implicit c: TestContext): Unit = okTest {
    val valueTable2 = Json.obj("de" -> "table2_de", "en" -> "table2_en")
    val valueTable4 = Json.obj("de" -> "table4_de", "en" -> "table4_en")

    for {
      tableId <- createUnionTable(true)
      cellValueRow21 <- sendRequest("GET", s"/tables/$tableId/columns/1/rows/2000001").map(_.getJsonObject("value"))
      cellValueRow41 <- sendRequest("GET", s"/tables/$tableId/columns/1/rows/4000001").map(_.getJsonObject("value"))
    } yield {
      assertJSONEquals(valueTable2, cellValueRow21)
      assertJSONEquals(valueTable4, cellValueRow41)
    }
  }
}
