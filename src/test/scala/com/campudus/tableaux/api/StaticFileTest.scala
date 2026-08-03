package com.campudus.tableaux.api

import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.*

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class StaticFileTest extends TableauxTestBase {

  @Test
  def checkIndexHtml(implicit c: TestContext): Unit = okTest {
    for {
      expected <- vertx.fileSystem().readFile("index.html").asScala
      actual <- sendStringRequest("GET", "/")
    } yield {
      assertEquals(expected.toString("utf-8"), actual)
    }
  }
}
