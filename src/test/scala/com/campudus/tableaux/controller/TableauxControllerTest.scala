package com.campudus.tableaux.controller

import com.campudus.tableaux.TestConfig
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.TableauxModel
import com.campudus.tableaux.database.model.TableauxModel._
import org.junit.Test
import org.vertx.scala.testtools.TestVerticle
import org.vertx.testtools.VertxAssert._

import scala.concurrent.Future

class TableauxControllerTest extends TestVerticle with TestConfig {

  override val verticle = this

  def createTableauxController(): TableauxController = {
    val dbConnection = DatabaseConnection(tableauxConfig)
    val model = TableauxModel(dbConnection)

    TableauxController(tableauxConfig, model)
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
    illegalArgumentTest(controller.createRow(0, Option(Seq(Seq((0: ColumnId, ""))))))
  }

  @Test
  def checkCreateRowWithNullValue(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.createRow(0, Option(Seq(Seq((1: ColumnId, null))))))
  }

  @Test
  def checkFillCellWithNullParameter(): Unit = {
    val controller = createTableauxController()
    illegalArgumentTest(controller.fillCell(0, 0, 0, null))
  }

  private def illegalArgumentTest(f: => Future[_]): Unit = {
    try f map { _ => fail("should get an IllegalArgumentException") } catch {
      case ex: IllegalArgumentException => testComplete()
      case x: Throwable => fail(s"should get an IllegalArgumentException, but got exception $x")
    }
  }
}
