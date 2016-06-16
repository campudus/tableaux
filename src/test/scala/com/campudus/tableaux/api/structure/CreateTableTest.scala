package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class CreateTableTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")
  val createTableJsonWithHiddenFlag = Json.obj("name" -> "Test Nr. 1", "hidden" -> true)
  val createTableJsonWithLangtags = Json.obj("name" -> "Test Nr. 1", "langtags" -> Json.arr("de-DE", "en-GB", "en-US"))

  @Test
  def createTableWithName(implicit c: TestContext): Unit = okTest {
    val baseExpected = Json.obj(
      "status" -> "ok",
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB")
    )
    val expectedJson1 = baseExpected.copy().put("id", 1).mergeIn(createTableJson)
    val expectedJson2 = baseExpected.copy().put("id", 2).mergeIn(createTableJson)

    for {
      test1 <- sendRequest("POST", "/tables", createTableJson)
      test2 <- sendRequest("POST", "/tables", createTableJson)
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
    val expectedJson1 = baseExpected.copy().put("id", 1).mergeIn(createTableJsonWithHiddenFlag)
    val expectedJson2 = baseExpected.copy().put("id", 2).mergeIn(createTableJsonWithHiddenFlag)

    for {
      test1 <- sendRequest("POST", "/tables", createTableJsonWithHiddenFlag)
      test2 <- sendRequest("POST", "/tables", createTableJsonWithHiddenFlag)
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
    val expectedJson1 = baseExpected.copy().put("id", 1).mergeIn(createTableJsonWithLangtags)
    val expectedJson2 = baseExpected.copy().put("id", 2).mergeIn(createTableJsonWithLangtags)

    for {
      test1Post <- sendRequest("POST", "/tables", createTableJsonWithLangtags)
      test2Post <- sendRequest("POST", "/tables", createTableJsonWithLangtags)

      test1Get <- sendRequest("GET", "/tables/1")
      test2Get <- sendRequest("GET", "/tables/2")
    } yield {
      assertEquals(expectedJson1, test1Post)
      assertEquals(expectedJson2, test2Post)

      assertEquals(test1Get, test1Post)
      assertEquals(test2Get, test2Post)
    }
  }

}