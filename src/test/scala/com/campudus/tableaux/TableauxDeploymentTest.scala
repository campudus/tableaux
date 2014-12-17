package com.campudus.tableaux

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.junit.Test
import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.json.Json
import org.vertx.scala.testtools.TestVerticle
import org.vertx.testtools.VertxAssert.fail
import org.vertx.testtools.VertxAssert.testComplete

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
class TableauxDeploymentTest extends TestVerticle {

  @Test
  def deployTest(): Unit = {
    container.deployModule(System.getProperty("vertx.modulename"), Json.obj(), 1, {
      case Success(id) => testComplete()
      case Failure(ex) =>
        logger.error("should not fail", ex)
        fail("should not fail")
    }: Try[String] => Unit)
  }

}
