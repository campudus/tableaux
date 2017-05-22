package com.campudus.tableaux.api.content

import com.campudus.tableaux.database.domain.{Cardinality, Constraint, DefaultCardinality}
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.testtools.RequestCreation.{Columns, LinkBiDirectionalCol, Rows}
import com.campudus.tableaux.testtools.TestCustomException
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.{Ignore, Test}
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import scala.concurrent.Future

sealed trait Helper {

  def findByNameInColumnsArray(columnName: String)(jsonArray: JsonArray): JsonObject = {
    import scala.collection.JavaConverters._

    jsonArray.asScala
      .collect({
        case obj: JsonObject => obj
      })
      .toSeq
      .find(_.getString("name") == columnName)
      .orNull
  }
}

@RunWith(classOf[VertxUnitRunner])
class LinkDeleteCascadeTest extends LinkTestBase with Helper {

  @Test
  def createLinkColumnWithDeleteCascade(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        columns = Columns(
          LinkBiDirectionalCol("deleteCascade", tableId2, Constraint(DefaultCardinality, deleteCascade = true))
        )

        // create bi-directional link column
        createdDeleteCascadeLinkColumnTable1 <- sendRequest("POST", s"/tables/$tableId1/columns", columns)
          .map(_.getJsonArray("columns"))
          .map(_.getJsonObject(0))

        // retrieve bi-directional link column from table 1
        retrieveDeleteCascadeLinkColumnTable1 <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))
          .map(findByNameInColumnsArray("deleteCascade"))

        // retrieve bi-directional backlink from table 2
        retrieveDeleteCascadeLinkColumnTable2 <- sendRequest("GET", s"/tables/$tableId2/columns")
          .map(_.getJsonArray("columns"))
          .map(findByNameInColumnsArray("table1"))
      } yield {
        assertContainsDeep(
          Constraint(DefaultCardinality, deleteCascade = true).getJson,
          createdDeleteCascadeLinkColumnTable1.getJsonObject("constraint")
        )

        assertContainsDeep(
          Constraint(DefaultCardinality, deleteCascade = true).getJson,
          retrieveDeleteCascadeLinkColumnTable1.getJsonObject("constraint")
        )

        assertContainsDeep(
          Constraint(DefaultCardinality, deleteCascade = false).getJson,
          retrieveDeleteCascadeLinkColumnTable2.getJsonObject("constraint")
        )
      }
    }
  }

  @Test
  def deleteRowWithDeleteCascadeShouldDeleteForeignRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))

        rowId <- sendRequest("POST",
                             s"/tables/$tableId1/rows",
                             Rows(columns, Json.obj("deleteCascade" -> Json.arr(1, 2))))
          .map(_.getJsonArray("rows"))
          .map(_.getJsonObject(0))
          .map(_.getLong("id"))

        _ <- sendRequest("DELETE", s"/tables/$tableId1/rows/$rowId")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(_.getJsonArray("rows"))
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(_.getJsonArray("rows"))
      } yield {
        assertEquals(2, rowsTable1.size())
        assertEquals(0, rowsTable2.size())
      }
    }
  }

  @Test
  def deleteForeignRowsWithDeleteCascadeShouldNotDeleteParentRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))

        // create parent row
        _ <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("deleteCascade" -> Json.arr(1, 2))))

        // delete foreign rows
        _ <- sendRequest("DELETE", s"/tables/$tableId2/rows/1")
        _ <- sendRequest("DELETE", s"/tables/$tableId2/rows/2")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(_.getJsonArray("rows"))
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(_.getJsonArray("rows"))
      } yield {
        // parent rows should still be there
        assertEquals(3, rowsTable1.size())
        assertEquals(0, rowsTable2.size())
      }
    }
  }

  @Test
  def deleteRowWithoutDeleteCascadeShouldNotDeleteForeignRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId1/columns",
          Columns(
            LinkBiDirectionalCol("deleteCascade", tableId2, Constraint(DefaultCardinality, deleteCascade = false))
          ))

        columns <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))

        _ <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("deleteCascade" -> Json.arr(1, 2))))

        _ <- sendRequest("DELETE", s"/tables/$tableId1/rows/1")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(_.getJsonArray("rows"))
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(_.getJsonArray("rows"))
      } yield {
        assertEquals(2, rowsTable1.size())
        assertEquals(2, rowsTable2.size())
      }
    }
  }

  @Test
  def deleteRowWithDeleteCascadeShouldNotDeleteForeignRowsIfStillInUse(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns").map(_.getJsonArray("columns"))

        rowId <- sendRequest("POST",
                             s"/tables/$tableId1/rows",
                             Rows(columns, Json.obj("deleteCascade" -> Json.arr(1, 2))))
          .map(_.getJsonArray("rows"))
          .map(_.getJsonObject(0))
          .map(_.getLong("id"))

        _ <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("deleteCascade" -> Json.arr(2))))

        _ <- sendRequest("DELETE", s"/tables/$tableId1/rows/$rowId")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(_.getJsonArray("rows"))
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(_.getJsonArray("rows"))
      } yield {
        assertEquals(3, rowsTable1.size())
        assertEquals(1, rowsTable2.size())

        // row 2 was still in use so only row was deleted
        assertEquals(2, rowsTable2.getJsonObject(0).getLong("id").longValue())
      }
    }
  }

  @Test
  def deleteRowWithDeleteCascadeShouldNotDeleteForeignRowsIfNotLinked(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        _ <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade")

        _ <- sendRequest("DELETE", s"/tables/$tableId1/rows/1")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(_.getJsonArray("rows"))
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(_.getJsonArray("rows"))
      } yield {
        assertEquals(1, rowsTable1.size())
        assertEquals(2, rowsTable2.size())
      }
    }
  }

  @Test
  def deleteRowWithDeleteCascadeWhichTriggersDeleteCascade(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        table1LinkColumnId <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade1")

        table2LinkColumnId <- createDeleteCascadeLinkColumn(tableId2, tableId1, "deleteCascade2")

        _ <- sendRequest("POST", s"/tables/$tableId1/rows")

        _ <- sendRequest("PUT",
                         s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1",
                         Json.obj("value" -> Json.arr(1, 2)))

        _ <- sendRequest("PUT",
                         s"/tables/$tableId2/columns/$table2LinkColumnId/rows/1",
                         Json.obj("value" -> Json.arr(2)))

        _ <- sendRequest("PUT",
                         s"/tables/$tableId2/columns/$table2LinkColumnId/rows/2",
                         Json.obj("value" -> Json.arr(3)))

        _ <- sendRequest("DELETE", s"/tables/$tableId1/rows/1")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(_.getJsonArray("rows"))
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(_.getJsonArray("rows"))
      } yield {
        assertEquals(0, rowsTable1.size())
        assertEquals(0, rowsTable2.size())
      }
    }
  }

  @Test
  def clearingLinkCellWithDeleteCascadeShouldDeleteForeignRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))

        rowId <- sendRequest("POST",
                             s"/tables/$tableId1/rows",
                             Rows(columns, Json.obj("deleteCascade" -> Json.arr(1, 2))))
          .map(_.getJsonArray("rows"))
          .map(_.getJsonObject(0))
          .map(_.getLong("id"))

        // clear cell
        _ <- sendRequest("DELETE", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(_.getJsonArray("rows"))
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(_.getJsonArray("rows"))
      } yield {
        assertEquals(3, rowsTable1.size())
        assertEquals(0, rowsTable2.size())
      }
    }
  }

  @Test
  def deleteLinkWithDeleteCascadeShouldDeleteForeignRows(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade1")

        columns <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))

        rowId <- sendRequest("POST",
                             s"/tables/$tableId1/rows",
                             Rows(columns, Json.obj("deleteCascade" -> Json.arr(1, 2))))
          .map(_.getJsonArray("rows"))
          .map(_.getJsonObject(0))
          .map(_.getLong("id"))

        // delete link from link cell with delete cascade
        _ <- sendRequest("DELETE", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId/link/1")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(_.getJsonArray("rows"))
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(_.getJsonArray("rows"))
      } yield {
        assertEquals(3, rowsTable1.size())
        assertEquals(1, rowsTable2.size())

        // row 2 should still be there
        assertEquals(2, rowsTable2.getJsonObject(0).getLong("id").longValue())
      }
    }
  }

  @Test
  @Ignore
  def deleteRowWithDeleteCascadeCycle(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        table1LinkColumnId <- createDeleteCascadeLinkColumn(tableId1, tableId2, "deleteCascade1")

        table2LinkColumnId <- createDeleteCascadeLinkColumn(tableId2, tableId1, "deleteCascade2")

        _ <- sendRequest("PUT",
                         s"/tables/$tableId1/columns/$table1LinkColumnId/rows/1",
                         Json.obj("value" -> Json.arr(1)))

        _ <- sendRequest("PUT",
                         s"/tables/$tableId2/columns/$table2LinkColumnId/rows/1",
                         Json.obj("value" -> Json.arr(1)))

        // This will currently end up in a endless loop
        _ <- sendRequest("DELETE", s"/tables/$tableId1/rows/1")

        rowsTable1 <- sendRequest("GET", s"/tables/$tableId1/rows").map(_.getJsonArray("rows"))
        rowsTable2 <- sendRequest("GET", s"/tables/$tableId2/rows").map(_.getJsonArray("rows"))
      } yield {
        assertEquals(1, rowsTable1.size())
        assertEquals(1, rowsTable2.size())
      }
    }
  }

  private def createDeleteCascadeLinkColumn(
      tableId: TableId,
      toTableId: TableId,
      columnName: String
  ): Future[ColumnId] = {
    val columns = Columns(
      LinkBiDirectionalCol(columnName, toTableId, Constraint(DefaultCardinality, deleteCascade = true))
    )

    sendRequest("POST", s"/tables/$tableId/columns", columns)
      .map(_.getJsonArray("columns"))
      .map(_.getJsonObject(0))
      .map(_.getLong("id"))
  }
}

@RunWith(classOf[VertxUnitRunner])
class LinkCardinalityTest extends LinkTestBase with Helper {

  @Test
  def createLinkColumnWithCardinality(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        columns = Columns(
          LinkBiDirectionalCol("cardinality", tableId2, Constraint(Cardinality(5, 8), deleteCascade = false))
        )

        // create bi-directional link column
        createdLinkColumnWithCardinalityTable1 <- sendRequest("POST", s"/tables/$tableId1/columns", columns)
          .map(_.getJsonArray("columns"))
          .map(_.getJsonObject(0))

        // retrieve bi-directional link column from table 1
        retrieveLinkColumnWithCardinalityTable1 <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))
          .map(findByNameInColumnsArray("cardinality"))

        // retrieve bi-directional backlink from table 2
        retrieveLinkColumnWithCardinalityTable2 <- sendRequest("GET", s"/tables/$tableId2/columns")
          .map(_.getJsonArray("columns"))
          .map(findByNameInColumnsArray("table1"))
      } yield {
        assertContainsDeep(
          Constraint(Cardinality(5, 8), deleteCascade = false).getJson,
          createdLinkColumnWithCardinalityTable1.getJsonObject("constraint")
        )

        assertContainsDeep(
          Constraint(Cardinality(5, 8), deleteCascade = false).getJson,
          retrieveLinkColumnWithCardinalityTable1.getJsonObject("constraint")
        )

        assertContainsDeep(
          Constraint(Cardinality(8, 5), deleteCascade = false).getJson,
          retrieveLinkColumnWithCardinalityTable2.getJsonObject("constraint")
        )
      }
    }
  }

  @Test
  def insertTwoRowsWhichPointToSameForeignRowsShouldFail(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createCardinaltyLinkColumn(tableId1, tableId2, "cardinality", 1, 2)

        columns <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))

        rowId <- sendRequest("POST",
                             s"/tables/$tableId1/rows",
                             Rows(columns, Json.obj("cardinality" -> Json.arr(1, 2))))
          .map(_.getJsonArray("rows"))
          .map(_.getJsonObject(0))
          .map(_.getLong("id"))

        _ <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns, Json.obj("cardinality" -> Json.arr(1, 2))))
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, "error.database.checkSize", _) => Future.successful(())
          })

        result <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId")
      } yield {
        assertContainsDeep(Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), result.getJsonArray("value"))
      }
    }
  }

  @Test
  def patchTwoRowsToPointToSameForeignRowsShouldFail(implicit c: TestContext): Unit = {
    import scala.collection.JavaConverters._

    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId1 <- createCardinaltyLinkColumn(tableId1, tableId2, "cardinality", 1, 2)

        linkColumnId2 <- sendRequest("GET", s"/tables/$tableId2/columns")
          .map(_.getJsonArray("columns"))
          .map(_.asScala.map(_.asInstanceOf[JsonObject]))
          .map(_.find(_.getString("name") == "table1").get)
          .map(_.getLong("id"))

        rowId1 <- sendRequest("POST", s"/tables/$tableId1/rows")
          .map(_.getLong("id"))
        rowId2 <- sendRequest("POST", s"/tables/$tableId1/rows")
          .map(_.getLong("id"))

        _ <- sendRequest("PATCH",
                         s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId1",
                         Json.obj("value" -> Json.arr(1, 2)))

        _ <- sendRequest("PATCH",
                         s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId2",
                         Json.obj("value" -> Json.arr(1, 2)))
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, "error.database.checkSize", _) => Future.successful(())
          })

        rowId3 <- sendRequest("POST", s"/tables/$tableId2/rows")
          .map(_.getLong("id"))

        _ <- sendRequest("PATCH",
                         s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId3",
                         Json.obj("value" -> Json.arr(rowId1, rowId2)))
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, "error.database.checkSize", _) => Future.successful(())
          })

        result <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId1")
      } yield {
        assertContainsDeep(Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), result.getJsonArray("value"))
      }
    }
  }

  @Test
  def retrieveForeignRowsOfLinkCell(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createCardinaltyLinkColumn(tableId1, tableId2, "cardinality", 1, 1)

        rowId1 <- sendRequest("POST", s"/tables/$tableId1/rows")
          .map(_.getLong("id"))

        resultCell <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1")

        resultForeignRows <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1/foreignRows")
      } yield {
        assertEquals(0, resultCell.getJsonArray("value").size())

        assertContainsDeep(Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), resultForeignRows.getJsonArray("rows"))
      }
    }
  }

  @Test
  def retrieveForeignRowsOfLinkCellWhichDidNotYetHitTheLimit(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createCardinaltyLinkColumn(tableId1, tableId2, "cardinality", 1, 2)

        rowId1 <- sendRequest("POST", s"/tables/$tableId1/rows")
          .map(_.getLong("id"))

        _ <- sendRequest("PATCH",
                         s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1",
                         Json.obj("value" -> Json.arr(2)))

        resultCell <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1")

        resultForeignRows <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1/foreignRows")
      } yield {
        assertContainsDeep(Json.arr(Json.obj("id" -> 2)), resultCell.getJsonArray("value"))

        assertContainsDeep(Json.arr(Json.obj("id" -> 1)), resultForeignRows.getJsonArray("rows"))
      }
    }
  }

  @Test
  def retrieveForeignRowsOfLinkCellWhichAlreadyHitTheLimit(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId1 <- createDefaultTable(name = "table1")
        tableId2 <- createDefaultTable(name = "table2", tableNum = 2)

        linkColumnId <- createCardinaltyLinkColumn(tableId1, tableId2, "cardinality", 1, 2)

        rowId1 <- sendRequest("POST", s"/tables/$tableId1/rows")
          .map(_.getLong("id"))

        _ <- sendRequest("PATCH",
                         s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1",
                         Json.obj("value" -> Json.arr(1, 2)))

        resultCell <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1")

        resultForeignRows <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/$rowId1/foreignRows")
      } yield {
        assertContainsDeep(Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), resultCell.getJsonArray("value"))

        assertEquals(0, resultForeignRows.getJsonArray("rows").size())
      }
    }
  }

  @Test
  def patchAndRetrieveWithHigherCardinality(implicit c: TestContext): Unit = {
    import scala.collection.JavaConverters._

    okTest {
      for {
        tableId1 <- createEmptyDefaultTable(name = "table1")
        tableId2 <- createEmptyDefaultTable(name = "table2", tableNum = 2)

        linkColumnId1 <- createCardinaltyLinkColumn(tableId1, tableId2, "cardinality", 2, 3)

        columns1 <- sendRequest("GET", s"/tables/$tableId1/columns")
          .map(_.getJsonArray("columns"))
        columns2 <- sendRequest("GET", s"/tables/$tableId2/columns")
          .map(_.getJsonArray("columns"))

        linkColumnId2 = columns2.asScala
          .map(_.asInstanceOf[JsonObject])
          .find(_.getString("name") == "table1")
          .get
          .getLong("id")
          .longValue()

        rowId11 <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns1, Json.obj("Test Column 1" -> "row11")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))
        rowId12 <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns1, Json.obj("Test Column 1" -> "row12")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))

        rowId21 <- sendRequest("POST", s"/tables/$tableId2/rows", Rows(columns1, Json.obj("Test Column 1" -> "row21")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))
        rowId22 <- sendRequest("POST", s"/tables/$tableId2/rows", Rows(columns1, Json.obj("Test Column 1" -> "row22")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))
        rowId23 <- sendRequest("POST", s"/tables/$tableId2/rows", Rows(columns1, Json.obj("Test Column 1" -> "row23")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))
        rowId24 <- sendRequest("POST", s"/tables/$tableId2/rows", Rows(columns1, Json.obj("Test Column 1" -> "row24")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))

        // let's do some real tests
        _ <- sendRequest("PATCH",
                         s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId11",
                         Json.obj("value" -> Json.arr(rowId22, rowId21, rowId23)))

        _ <- sendRequest("PATCH",
                         s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId23",
                         Json.obj("value" -> Json.arr(rowId12)))

        resultCell11 <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId11")

        resultForeignRows11 <- sendRequest("GET",
                                           s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId11/foreignRows")

        resultCell21 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId21")
        resultCell22 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId22")
        resultCell23 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId23")

        resultForeignRows21 <- sendRequest("GET",
                                           s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId21/foreignRows")
        resultForeignRows23 <- sendRequest("GET",
                                           s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId23/foreignRows")
        resultForeignRows12 <- sendRequest("GET",
                                           s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId12/foreignRows")

        _ <- sendRequest("PATCH",
                         s"/tables/$tableId1/columns/$linkColumnId1/rows/$rowId11",
                         Json.obj("value" -> Json.arr(rowId24)))
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, "error.database.checkSize", _) => Future.successful(())
          })

        rowId13 <- sendRequest("POST", s"/tables/$tableId1/rows", Rows(columns1, Json.obj("Test Column 1" -> "row13")))
          .map(_.getJsonArray("rows").getJsonObject(0))
          .map(_.getInteger("id"))

        _ <- sendRequest("PATCH",
                         s"/tables/$tableId2/columns/$linkColumnId2/rows/$rowId23",
                         Json.obj("value" -> Json.arr(rowId13)))
          .flatMap(_ => Future.failed(new Exception("this request should fail")))
          .recoverWith({
            case TestCustomException(_, "error.database.checkSize", _) => Future.successful(())
          })
      } yield {
        assertContainsDeep(Json.arr(
                             Json.obj("id" -> rowId22),
                             Json.obj("id" -> rowId21),
                             Json.obj("id" -> rowId23)
                           ),
                           resultCell11.getJsonArray("value"))

        // cell 11 is already at it's limit
        assertEquals(0, resultForeignRows11.getJsonArray("rows").size())

        assertContainsDeep(Json.arr(Json.obj("id" -> rowId11)), resultCell21.getJsonArray("value"))
        assertContainsDeep(Json.arr(Json.obj("id" -> rowId11)), resultCell22.getJsonArray("value"))
        assertContainsDeep(Json.arr(Json.obj("id" -> rowId11), Json.obj("id" -> rowId12)),
                           resultCell23.getJsonArray("value"))

        assertContainsDeep(Json.arr(Json.obj("id" -> rowId12)), resultForeignRows21.getJsonArray("rows"))

        // cell 23 is already at it's limit
        assertEquals(0, resultForeignRows23.getJsonArray("rows").size())

        assertContainsDeep(Json.arr(
                             Json.obj("id" -> rowId21),
                             Json.obj("id" -> rowId22),
                             Json.obj("id" -> rowId24)
                           ),
                           resultForeignRows12.getJsonArray("rows"))
      }
    }
  }

  private def createCardinaltyLinkColumn(
      tableId: TableId,
      toTableId: TableId,
      columnName: String,
      from: Int,
      to: Int
  ): Future[ColumnId] = {
    val columns = Columns(
      LinkBiDirectionalCol(columnName, toTableId, Constraint(Cardinality(from, to), deleteCascade = false))
    )

    sendRequest("POST", s"/tables/$tableId/columns", columns)
      .map(_.getJsonArray("columns"))
      .map(_.getJsonObject(0))
      .map(_.getLong("id"))
  }
}
