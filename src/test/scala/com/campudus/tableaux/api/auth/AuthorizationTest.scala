package com.campudus.tableaux.api.auth

import com.campudus.tableaux.testtools.{TableauxTestBase, TokenHelper}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonObject}

@RunWith(classOf[VertxUnitRunner])
class AuthorizationTest extends TableauxTestBase {

  /*
   * For integration tests we are forced to use only one role-permissions configuration because
   * "TableauxTestBase" class manages the vert.x startup in a @Before TestSetup method and for
   * now it is not possible to inject different setting on runtime.
   *
   * Therefore the mass and detailed tests are performed as unit test on controller layer and
   * additionally some base tests with complete REST workflow are located here.
   */

  val defaultTestClaims: JsonObject = Json.obj("aud" -> "grud-backend", "iss" -> "campudus-test")

  def tokenWithRoles(roles: String*): Option[String] = {
    val tokenHelper: TokenHelper = TokenHelper(this.vertxAccess())

    val claims: JsonObject = defaultTestClaims.put("realm_access", Json.obj("roles" -> roles))

    Some(tokenHelper.generateToken(claims))
  }

  val expectedOkJson: JsonObject = Json.obj("status" -> "ok")

  @Test
  def testAuthorization_tokenSignedWithDifferentKey_unauthorized(implicit c: TestContext): Unit = {
    exceptionTest("Unauthorized") {
      val token: String = "" +
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJjYW1wdWR1cy10ZXN0Iiwic3ViIjoidGVzdEBjYW1wdWR1cy5jb20iLCJhdWQiOiJnc" +
        "nVkLWJhY2tlbmQiLCJuYmYiOjAsImlhdCI6MTU1NzMyODIwNywiZXhwIjoyMjIyMjIyMjIyLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJ1bml0LXRlc3R" +
        "lciIsImFjciI6IjEiLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsiZGV2ZWxvcGVyIiwidmlldy10YWJsZXMiLCJkZWxldGUtbWVkaWEiLCJ2aWV3L" +
        "WNlbGxzIl19LCJzY29wZSI6ImVtYWlsIHByb2ZpbGUiLCJuYW1lIjoiVGVzdCBUZXN0IiwicHJlZmVycmVkX3VzZXJuYW1lIjoiVGVzdCIsImdpdmV" +
        "uX25hbWUiOiJUZXN0IiwiZmFtaWx5X25hbWUiOiJUZXN0In0.YrJ4ikXxjRBITp9B98lc-ygr7Xlc52PKSCnSU1G3YWOxec9DJH0ybkGdwSYqVLejQ" +
        "5PC12CVlh19IAEHON2lXTPmMAMOoOlG5dcvTs6MSYnYwoJnTE91MJ0yUJHRcmSkC6npbYnsYjwzk_UKgXcmKYW6UMrsIcU1bEImXWNoLtU"

      for {
        _ <- sendRequest("GET", "/system/versions", Some(token))
      } yield ()
    }
  }

  @Test
  def testAuthorization_invalidAudience_unauthorized(implicit c: TestContext): Unit = {
    exceptionTest("Unauthorized") {
      val tokenHelper: TokenHelper = TokenHelper(this.vertxAccess())

      val token: String = tokenHelper.generateToken(Json.obj("aud" -> "__invalid__"))

      for {
        _ <- sendRequest("GET", "/system/versions", Some(token))
      } yield ()
    }
  }

  @Test
  def testAuthorization_invalidIssuer_unauthorized(implicit c: TestContext): Unit = {
    exceptionTest("Unauthorized") {
      val tokenHelper: TokenHelper = TokenHelper(this.vertxAccess())

      val token: String = tokenHelper.generateToken(Json.obj("aud" -> "grud-backend", "iss" -> "__invalid__"))

      for {
        _ <- sendRequest("GET", "/system/versions", Some(token))
      } yield ()
    }
  }

  @Test
  def testAuthorization_expirationTimeLiesInThePast_unauthorized(implicit c: TestContext): Unit = {
    exceptionTest("Unauthorized") {
      val tokenHelper: TokenHelper = TokenHelper(this.vertxAccess())

      import java.time.Instant
      val timestampOneMinuteAgo: Long = Instant.now.getEpochSecond - 60

      val token: String =
        tokenHelper.generateToken(
          Json.obj("aud" -> "grud-backend", "iss" -> "campudus-test", "exp" -> timestampOneMinuteAgo))

      for {
        _ <- sendRequest("GET", "/system/versions", Some(token))
      } yield ()
    }
  }

  @Test
  def testAuthorization__correctSignedToken_validAudience_validIssuer__ok(implicit c: TestContext): Unit = {
    okTest {
      val tokenHelper: TokenHelper = TokenHelper(this.vertxAccess())

      val token: String = tokenHelper.generateToken(Json.obj("aud" -> "grud-backend", "iss" -> "campudus-test"))

      for {
        _ <- sendRequest("GET", "/system/versions", Some(token))
      } yield ()
    }
  }

  @Test
  def deleteTable_validRole(implicit c: TestContext): Unit = {
    okTest {

      def createTableJson: JsonObject = {
        Json.obj("name" -> s"Test Table")
      }

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test <- sendRequest("DELETE", "/tables/1", tokenWithRoles("delete-tables", "view-tables"))
      } yield {
        assertEquals(expectedOkJson, test)
      }
    }
  }

  @Test
  def deleteTable_withoutRole(implicit c: TestContext): Unit = {
    exceptionTest("error.request.unauthorized") {

      def createTableJson: JsonObject = {
        Json.obj("name" -> s"Test Table")
      }

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        test <- sendRequest("DELETE", "/tables/1", tokenWithRoles())
      } yield {
        assertEquals(expectedOkJson, test)
      }
    }
  }

}
