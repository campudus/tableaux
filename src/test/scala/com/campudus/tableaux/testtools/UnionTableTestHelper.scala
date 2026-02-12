package com.campudus.tableaux.testtools

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.Cardinality
import com.campudus.tableaux.database.domain.Constraint
import com.campudus.tableaux.database.model.TableauxModel.ColumnId
import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.testtools.RequestCreation._

import io.vertx.core.json._
import org.vertx.scala.core.json.Json

import scala.collection.JavaConverters._
import scala.concurrent.Future

trait UnionTableTestHelper extends TableauxTestBase {

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
