package com.campudus.tableaux.api.auth

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonObject}

@RunWith(classOf[VertxUnitRunner])
class AuthorizationTest extends TableauxTestBase {

  val expectedOkJson: JsonObject = Json.obj("status" -> "ok")

  @Test
  def ffff(implicit c: TestContext): Unit = {
    okTest {

      def createTableJson: JsonObject = {
        Json.obj("name" -> s"Test Table")
      }

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test <- sendRequest("DELETE", "/tables/1")
      } yield {
        assertEquals(expectedOkJson, test)
      }
    }
  }

//  @Test
//  def testAuthorization_expirationTimeLiesInThePast_unauthorized(implicit c: TestContext): Unit = {
//    exceptionTest("Unauthorized") {
//      val tokenHelper = TokenHelper(this.vertxAccess())
//
//      import java.time.Instant
//      val timestampOneMinuteAgo: Long = Instant.now.getEpochSecond - 60
//
//      val token =
//        tokenHelper.generateToken(
//          Json.obj("aud" -> "grud-backend", "iss" -> "campudus-test", "exp" -> timestampOneMinuteAgo))
//
//      for {
//        _ <- sendRequest("GET", "/system/versions", Some(token))
//      } yield ()
//    }
//  }
//
//  @Test
//  def testAuthorization__correctSignedToken_validAudience_validIssuer__ok(implicit c: TestContext): Unit = {
//    okTest {
//      val tokenHelper = TokenHelper(this.vertxAccess())
//
//      val token = tokenHelper.generateToken(Json.obj("aud" -> "grud-backend", "iss" -> "campudus-test"))
//
//      for {
//        _ <- sendRequest("GET", "/system/versions", Some(token))
//      } yield ()
//    }
//  }
}
