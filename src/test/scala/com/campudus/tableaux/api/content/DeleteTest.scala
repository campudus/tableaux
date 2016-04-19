package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class DeleteTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")

  val expectedOkJson = Json.obj("status" -> "ok")

  @Test
  def deleteRow(implicit c: TestContext): Unit = okTest {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("DELETE", "/tables/1/rows/1")
    } yield {
      assertEquals(expectedOkJson, test)
    }
  }
}