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
    illegalArgumentTest(controller.createTable(null))
  }

  @Test
  def checkCreateLinkColumnWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    illegalArgumentTest(controller.createColumn(0, null, null, 0, 0, 0))
  }

  @Test
  def checkCreateColumnWithEmptyParameter(): Unit = {
    val controller = new TableauxController(this)
    illegalArgumentTest(controller.createColumn(0, Seq()))
  }

  @Test
  def checkCreateColumnWithNullName(): Unit = {
    val controller = new TableauxController(this)
    illegalArgumentTest(controller.createColumn(0, Seq((null, ""))))
  }

  @Test
  def checkCreateColumnWithNullType(): Unit = {
    val controller = new TableauxController(this)
    illegalArgumentTest(controller.createColumn(0, Seq(("", null))))
  }

  @Test
  def checkCreateRowWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    illegalArgumentTest(controller.createRow(0, Option(Seq())))
  }

  @Test
  def checkCreateRowWithNullSeq(): Unit = {
    val controller = new TableauxController(this)
    illegalArgumentTest(controller.createRow(0, Option(Seq(Seq()))))
  }

  @Test
  def checkCreateRowWithNullId: Unit = {
    val controller = new TableauxController(this)
    illegalArgumentTest(controller.createRow(0, Option(Seq(Seq((0, ""))))))
  }

  @Test
  def checkCreateRowWithNullValue: Unit = {
    val controller = new TableauxController(this)
    illegalArgumentTest(controller.createRow(0, Option(Seq(Seq((1, null))))))
  }

  @Test
  def checkDeleteColumnWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    illegalArgumentTest(controller.deleteColumn(0, 0))
  }

  @Test
  def checkDeleteTableWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    illegalArgumentTest(controller.deleteTable(0))
  }

  @Test
  def checkFillCellWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    illegalArgumentTest(controller.fillCell(0, 0, 0, null, null))
  }

  @Test
  def checkGetColumnWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    illegalArgumentTest(controller.getColumn(0, 0))
  }

  @Test
  def checkGetTableWithNullParameter(): Unit = {
    val controller = new TableauxController(this)
    illegalArgumentTest(controller.getTable(0))
  }

  private def illegalArgumentTest(f: => Future[_]): Unit = {
    try f map { _ => fail("should get an IllegalArgumentException") } catch {
      case ex: IllegalArgumentException => testComplete()
      case x: Throwable => fail(s"should get an IllegalArgumentException, but got exception $x")
    }
  }

}