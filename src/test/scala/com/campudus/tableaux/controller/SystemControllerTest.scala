package com.campudus.tableaux.controller

import com.campudus.tableaux.TableauxTestBase
import com.campudus.tableaux.router.SystemRouter
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
    val expectedJson1 = Json.obj(
      "status" -> "ok",
      "tables" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Bundesländer", "hidden" -> false),
        Json.obj("id" -> 2, "name" -> "Regierungsbezirke", "hidden" -> false)
      )
    )

    val expectedJson2 = Json.obj(
      "status" -> "ok",
      "tables" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Bundesländer", "hidden" -> false),
        Json.obj("id" -> 2, "name" -> "Regierungsbezirke", "hidden" -> false)
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
        "current" -> 8,
        "specification" -> 8
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
  def resetWithOutNonce(implicit c: TestContext): Unit = exceptionTest("error.nonce.none") {
    SystemRouter.nonce = null
    sendRequest("POST", s"/system/reset")
  }

  @Test
  def resetWithNonceButInvalidRequestNonce(implicit c: TestContext): Unit = exceptionTest("error.nonce.invalid") {
    val nonce = SystemRouter.generateNonce()
    sendRequest("POST", s"/system/reset?nonce=asdf")
  }
}
