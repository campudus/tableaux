package com.campudus.tableaux.controller

import com.campudus.tableaux.router.SystemRouter
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class SystemControllerTest extends TableauxTestBase {

  @Test
  def resetSystemTables(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok")

    val nonce = SystemRouter.generateNonce()

    for {
      result <- sendRequest("POST", s"/system/reset?nonce=$nonce")
    } yield {
      assertEquals(expectedJson, result)
    }
  }

  @Test
  def resetDemoData(implicit c: TestContext): Unit = okTest {
    val baseTable = Json.obj(
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB")
    )
    val expectedJson1 = Json.obj(
      "status" -> "ok",
      "tables" -> Json.arr(
        baseTable.copy().mergeIn(Json.obj("id" -> 1, "name" -> "Bundesländer")),
        baseTable.copy().mergeIn(Json.obj("id" -> 2, "name" -> "Regierungsbezirke"))
      )
    )

    val expectedJson2 = Json.obj(
      "status" -> "ok",
      "tables" -> Json.arr(
        baseTable.copy().mergeIn(Json.obj("id" -> 1, "name" -> "Bundesländer")),
        baseTable.copy().mergeIn(Json.obj("id" -> 2, "name" -> "Regierungsbezirke"))
      )
    )

    for {
      _ <- {
        val nonce = SystemRouter.generateNonce()
        sendRequest("POST", s"/system/reset?nonce=$nonce")
      }
      result <- {
        val nonce = SystemRouter.generateNonce()
        sendRequest("POST", s"/system/resetDemo?nonce=$nonce")
      }
      tablesResult <- sendRequest("GET", "/tables")
    } yield {
      assertEquals(expectedJson1, result)
      assertEquals(expectedJson2, tablesResult)
    }
  }

  @Test
  def retrieveVersions(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj(
      "database" -> Json.obj(
        "current" -> 17,
        "specification" -> 17
      )
    )

    for {
      _ <- {
        val nonce = SystemRouter.generateNonce()
        sendRequest("POST", s"/system/reset?nonce=$nonce")
      }
      _ <- {
        val nonce = SystemRouter.generateNonce()
        sendRequest("POST", s"/system/resetDemo?nonce=$nonce")
      }
      versions <- sendRequest("GET", "/system/versions")
    } yield {
      assertContains(expectedJson, versions.getJsonObject("versions"))
    }
  }

  @Test
  def resetWithInvalidatedNonceAndNoRequestNonce(implicit c: TestContext): Unit = {
    exceptionTest("error.nonce.none"){
      SystemRouter.invalidateNonce()
      sendRequest("POST", s"/system/reset")
    }
  }

  @Test
  def resetWithNonceAndNoRequestNonce(implicit c: TestContext): Unit = {
    exceptionTest("error.nonce.invalid"){
      SystemRouter.generateNonce()
      sendRequest("POST", s"/system/reset")
    }
  }

  @Test
  def resetWithNonceButInvalidRequestNonce(implicit c: TestContext): Unit = {
    exceptionTest("error.nonce.invalid"){
      SystemRouter.generateNonce()
      sendRequest("POST", s"/system/reset?nonce=asdf")
    }
  }

  @Test
  def resetWithInvalidatedNonceAndInvalidRequestNonce(implicit c: TestContext): Unit = {
    exceptionTest("error.nonce.none"){
      SystemRouter.invalidateNonce()
      sendRequest("POST", s"/system/reset?nonce=asdf")
    }
  }

  @Test
  def resetWithNonceAndInvalidRequestNonce(implicit c: TestContext): Unit = {
    exceptionTest("error.nonce.invalid"){
      SystemRouter.generateNonce()
      sendRequest("POST", s"/system/reset?nonce=asdf")
    }
  }

  @Test
  def resetInDevModeWithInvalidRequestNonce(implicit c: TestContext): Unit = {
    okTest{
      val expectedJson = Json.obj("status" -> "ok")

      SystemRouter.generateNonce()
      SystemRouter.setDevMode(true)

      for {
        result <- sendRequest("POST", s"/system/reset?nonce=asdf")
      } yield {
        assertEquals(expectedJson, result)
        assertTrue(SystemRouter.isDevMode)
        SystemRouter.setDevMode(false)
        assertFalse(SystemRouter.isDevMode)
      }
    }
  }
}
