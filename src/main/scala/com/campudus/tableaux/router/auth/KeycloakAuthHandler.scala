package com.campudus.tableaux.router.auth

import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.{AuthenticationException, TableauxConfig}
import io.vertx.core.Handler
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.auth.User
import io.vertx.scala.ext.auth.oauth2.KeycloakHelper
import io.vertx.scala.ext.web.RoutingContext

import scala.collection.JavaConverters._
import org.vertx.scala.core.json.Json
import com.campudus.tableaux.helper.JsonUtils
import scala.util.Try

object KeycloakAuthHandler {
  val TOKEN_PAYLOAD: String = "tokenPayload"

  def apply(vertx: Vertx, config: TableauxConfig): KeycloakAuthHandler = {
    new KeycloakAuthHandler(vertx, config)
  }
}

class KeycloakAuthHandler(override val vertx: Vertx, tableauxConfig: TableauxConfig)
    extends Handler[RoutingContext] with VertxAccess {

  /**
    * Validates Access Token's "aud" and "iss" claims (https://tools.ietf.org/html/rfc7519). If successful it stores the
    * principal in requestContext for authorization. If not context fails with statusCode 401.
    */
  override def handle(rc: RoutingContext): Unit = {
    val user: Option[User] = rc.user()

    val tokenPayload = user match {
      case Some(u) => KeycloakHelper.accessToken(u.principal())
      case _ =>
        val exception = AuthenticationException("No user in context")
        logger.error(exception.getMessage)
        rc.fail(exception.statusCode, exception)
        throw exception
    }

    checkAudience(rc, tokenPayload)
    checkIssuer(rc, tokenPayload)

    rc.put(KeycloakAuthHandler.TOKEN_PAYLOAD, tokenPayload)
    rc.next()
  }

  private def checkAudience(context: RoutingContext, tokenPayload: JsonObject): Unit = {
    if (shouldVerifyAudience) {
      val audiences: Seq[String] = tokenPayload.getValue("aud") match {
        case s: String => Seq(s)
        case o: JsonArray => o.asScala.map({ case item: String => item }).toSeq
      }

      if (!audiences.contains(getAudience)) {
        val exception = AuthenticationException(
          s"Audiences $audiences of the request doesn't contain configured service audience (resource) '$getAudience'"
        )
        logger.error(exception.getMessage)
        context.fail(exception.statusCode, exception)
        throw exception
      }
    }
  }

  private def checkIssuer(context: RoutingContext, tokenPayload: JsonObject): Unit = {
    val issuer: String = tokenPayload.getString("iss", "_invalid_")

    if (issuer != getIssuer) {
      val exception = AuthenticationException(
        s"Issuer '$issuer' of the request doesn't match to configured service issuer '$getIssuer'"
      )
      logger.error(exception.getMessage)
      context.fail(exception.statusCode, exception)
      throw exception
    }
  }

  def getAudience: String = tableauxConfig.authConfig.getString("resource")
  def shouldVerifyAudience: Boolean = tableauxConfig.authConfig.getBoolean("verify-token-audience", false)
  def getIssuer: String = tableauxConfig.authConfig.getString("issuer")
}
