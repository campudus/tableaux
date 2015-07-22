package com.campudus.tableaux

import com.campudus.tableaux.controller.{StructureController, TableauxController}
import com.campudus.tableaux.database.model.{StructureModel, TableauxModel}
import TableauxModel._
import com.campudus.tableaux.database.domain.{CreateSimpleColumn}
import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.testtools.TestVerticle
import scala.concurrent.Future
import com.campudus.tableaux.database.{SingleLanguage, TextType, DatabaseConnection}

class ControllerTest extends TestVerticle with TestConfig {

  override val verticle = this

  def createTableauxController(): TableauxController = {
    val dbConnection = DatabaseConnection(tableauxConfig)
    val model = TableauxModel(dbConnection)

    TableauxController(tableauxConfig, model)
  }

  /*
  @Test
  def checkCreateTableWithNullParameter(): Unit = {
    val controller = createStructureController()
    illegalArgumentTest(controller.createTable(null))
  }

  @Test
  def checkCreateLinkColumnWithNullParameter(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createColumn(0, Seq(CreateSimpleColumn(null, null, null, SingleLanguage))))
  }

  @Test
  def checkCreateColumnWithEmptyParameter(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createColumn(0, Seq()))
  }

  @Test
  def checkCreateColumnWithNullName(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createColumn(0, Seq(CreateSimpleColumn(null, None, TextType, SingleLanguage))))
  }

  @Test
  def checkCreateColumnWithNullType(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createColumn(0, Seq(CreateSimpleColumn("", None, null, SingleLanguage))))
  }
  */

  @Test
  def checkCreateRowWithNullParameter(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createRow(0, Option(Seq())))
  }

  @Test
  def checkCreateRowWithNullSeq(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createRow(0, Option(Seq(Seq()))))
  }

  @Test
  def checkCreateRowWithNullId(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createRow(0, Option(Seq(Seq((0: ColumnId, ""))))))
  }

  @Test
  def checkCreateRowWithNullValue(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createRow(0, Option(Seq(Seq((1: ColumnId, null))))))
  }

  /*
  @Test
  def checkDeleteColumnWithNullParameter(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.deleteColumn(0, 0))
  }

  @Test
  def checkDeleteTableWithNullParameter(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.deleteTable(0))
  }
  */

  @Test
  def checkFillCellWithNullParameter(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.fillCell(0, 0, 0, null))
  }

  /*
  @Test
  def checkGetColumnWithNullParameter(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.getColumn(0, 0))
  }

  @Test
  def checkGetTableWithNullParameter(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.getTable(0))
  }
  */

  private def illegalArgumentTest(f: => Future[_]): Unit = {
    try f map { _ => fail("should get an IllegalArgumentException") } catch {
      case ex: IllegalArgumentException => testComplete()
      case x: Throwable => fail(s"should get an IllegalArgumentException, but got exception $x")
    }
  }
}
