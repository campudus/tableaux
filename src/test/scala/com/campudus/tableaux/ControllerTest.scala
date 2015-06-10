package com.campudus.tableaux

import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.database.model.{SystemModel, TableauxModel}
import TableauxModel._
import com.campudus.tableaux.database.domain.CreateColumn
import org.junit.Test
import org.vertx.scala.core.json.{Json, JsonObject}
import org.vertx.scala.platform.Verticle
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.testtools.TestVerticle
import scala.io.Source
import scala.util.Try
import scala.concurrent.Future
import scala.util.Success
import scala.util.Failure
import com.campudus.tableaux.database.{TextType, DatabaseConnection}

class ControllerTest extends TestVerticle with TestConfig {

  override val verticle = this

  def createTableauxController(): TableauxController = {
    val dbConnection = DatabaseConnection(tableauxConfig)
    val model = TableauxModel(dbConnection)
    
    TableauxController(tableauxConfig, model)
  }
  
  @Test
  def checkCreateTableWithNullParameter(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createTable(null))
  }

  @Test
  def checkCreateLinkColumnWithNullParameter(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createColumn(0, Seq(CreateColumn(null, null, null, None))))
  }

  @Test
  def checkCreateColumnWithEmptyParameter(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createColumn(0, Seq()))
  }

  @Test
  def checkCreateColumnWithNullName(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createColumn(0, Seq(CreateColumn(null, TextType, None, None))))
  }

  @Test
  def checkCreateColumnWithNullType(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createColumn(0, Seq(CreateColumn("", null, None, None))))
  }

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
    illegalArgumentTest(controller.createRow(0: IdType, Option(Seq(Seq((0: IdType, ""))))))
  }

  @Test
  def checkCreateRowWithNullValue(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createRow(0: IdType, Option(Seq(Seq((1: IdType, null))))))
  }

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

  @Test
  def checkFillCellWithNullParameter(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.fillCell(0, 0, 0, null))
  }

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

  private def illegalArgumentTest(f: => Future[_]): Unit = {
    try f map { _ => fail("should get an IllegalArgumentException") } catch {
      case ex: IllegalArgumentException => testComplete()
      case x: Throwable => fail(s"should get an IllegalArgumentException, but got exception $x")
    }
  }
}
