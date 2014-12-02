package com.campudus.tableaux

import java.nio.file.{FileSystem, Path, Files}

import org.junit.Test
import org.vertx.scala.core.FunctionConverters._

import org.vertx.scala.core.json.Json
import org.vertx.scala.testtools.TestVerticle
import org.vertx.testtools.VertxAssert._

import collection.JavaConverters._
import scala.tools.nsc.io

import scala.util.{Failure, Success, Try}

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
class TableauxCreationTest extends TestVerticle {

  @Test
  def deployTest(): Unit = {
    println(s"module name=${System.getProperty("vertx.modulename")}")
    println(s"mods dir=${System.getProperty("vertx.mods")}")
    vertx.fileSystem.readDirSync(System.getProperty("vertx.mods")).map(println)
    container.deployModule(System.getProperty("vertx.modulename"), Json.obj(), 1, {
      case Success(id) => testComplete()
      case Failure(ex) =>
        logger.error("should not fail", ex)
        fail("should not fail")
    }: Try[String] => Unit)
  }

}
