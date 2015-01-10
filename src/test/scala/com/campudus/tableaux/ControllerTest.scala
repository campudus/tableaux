package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.testtools.TestVerticle
import scala.util.Try
import scala.concurrent.Future
import scala.util.Success
import scala.util.Failure

class ControllerTest extends TestVerticle {

  @Test
  def checkCreateTableWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    okTest(controller.createTable(null))
  }

  @Test
  def checkCreateLinkColumnWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    okTest(controller.createColumn(0, null, null, 0, 0, 0))
  }

  @Test
  def checkCreateColumnWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    okTest(controller.createColumn(0, null, null))
  }

  @Test
  def checkCreateRowWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    okTest(controller.createRow(0))
  }

  @Test
  def checkDeleteColumnWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    okTest(controller.deleteColumn(0, 0))
  }

  @Test
  def checkDeleteTableWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    okTest(controller.deleteTable(0))
  }

  @Test
  def checkFillCellWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    okTest(controller.fillCell(0, 0, 0, null, null))
  }

  @Test
  def checkGetColumnWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    okTest(controller.getColumn(0, 0))
  }

  @Test
  def checkGetTableWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    okTest(controller.getTable(0))
  }

  private def okTest(f: => Future[_]): Unit = {
    try f map { _ => fail("should not work with null values") } catch {
      case ex: IllegalArgumentException => testComplete()
      case _: Throwable                 => fail("should get an InvalidArgumentException")
    }
  }

}