package com.campudus.tableaux.api.auth

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class TokenTest extends TableauxTestBase {

  @Test
  def testAuthorization_missingToken_unauthorized(implicit c: TestContext): Unit = {
    exceptionTest("Unauthorized") {
      for {
        _ <- sendRequest("GET", "/system/versions")
      } yield ()
    }
  }

  @Test
  def testAuthorization_tokenSignedWithDifferentKey_unauthorized(implicit c: TestContext): Unit = {
    exceptionTest("Unauthorized") {
      val token = "" +
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
}
