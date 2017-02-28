package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase
import org.vertx.scala.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

import scala.util.Random

@RunWith(classOf[VertxUnitRunner])
class CreateTableTest extends TableauxTestBase {

  def createTableJson: JsonObject = {
    val random = Random.nextInt()
    Json.obj("name" -> s"Test Nr. $random")
  }

  def createTableJsonWithHiddenFlag: JsonObject = {
    val random = Random.nextInt()
    Json.obj("name" -> s"Test Nr. $random", "hidden" -> true)
  }

  def createTableJsonWithLangtags: JsonObject = {
    val random = Random.nextInt()
    Json.obj("name" -> s"Test Nr. $random", "langtags" -> Json.arr("de-DE", "en-GB", "en-US"))
  }

  @Test
  def createTableWithName(implicit c: TestContext): Unit = okTest {
    val baseExpected = Json.obj(
      "status" -> "ok",
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB")
    )

    val createTable1 = createTableJson
    val createTable2 = createTableJson

    val expectedJson1 = baseExpected.copy().put("id", 1).mergeIn(createTable1)
    val expectedJson2 = baseExpected.copy().put("id", 2).mergeIn(createTable2)

    for {
      test1 <- sendRequest("POST", "/tables", createTable1)
      test2 <- sendRequest("POST", "/tables", createTable2)
    } yield {
      assertEquals(expectedJson1, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createTableWithHiddenFlag(implicit c: TestContext): Unit = okTest {
    val baseExpected = Json.obj(
      "status" -> "ok",
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB")
    )

    val createTable1 = createTableJsonWithHiddenFlag
    val createTable2 = createTableJsonWithHiddenFlag

    val expectedJson1 = baseExpected.copy().put("id", 1).mergeIn(createTable1)
    val expectedJson2 = baseExpected.copy().put("id", 2).mergeIn(createTable2)

    for {
      test1 <- sendRequest("POST", "/tables", createTable1)
      test2 <- sendRequest("POST", "/tables", createTable2)
    } yield {
      assertEquals(expectedJson1, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createTableWithLangtags(implicit c: TestContext): Unit = okTest {
    val baseExpected = Json.obj(
      "status" -> "ok",
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj()
    )

    val createTable1 = createTableJsonWithLangtags
    val createTable2 = createTableJsonWithLangtags

    val expectedJson1 = baseExpected.copy().put("id", 1).mergeIn(createTable1)
    val expectedJson2 = baseExpected.copy().put("id", 2).mergeIn(createTable2)

    for {
      test1Post <- sendRequest("POST", "/tables", createTable1)
      test2Post <- sendRequest("POST", "/tables", createTable2)

      test1Get <- sendRequest("GET", "/tables/1")
      test2Get <- sendRequest("GET", "/tables/2")
    } yield {
      assertEquals(expectedJson1, test1Post)
      assertEquals(expectedJson2, test2Post)

      assertEquals(test1Get, test1Post)
      assertEquals(test2Get, test2Post)
    }
  }

  @Test
  def createTablesWithSameName(implicit c: TestContext): Unit = exceptionTest("error.request.unique.table") {
    for {
      _ <- sendRequest("POST", "/tables", Json.obj("name" -> s"Test"))
      _ <- sendRequest("POST", "/tables", Json.obj("name" -> s"Test"))
    } yield ()
  }

}