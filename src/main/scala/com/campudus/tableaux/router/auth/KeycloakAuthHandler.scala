package com.campudus.tableaux.router.auth

import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.{AuthenticationException, RequestContext, TableauxConfig}
import io.vertx.core.Handler
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.auth.User
import io.vertx.scala.ext.auth.oauth2.KeycloakHelper
import io.vertx.scala.ext.web.RoutingContext

import scala.collection.JavaConverters._

class KeycloakAuthHandler(
    override val vertx: Vertx,
    tableauxConfig: TableauxConfig
)(implicit requestContext: RequestContext)
    extends Handler[RoutingContext]
    with VertxAccess {

  /**
    * Validates Access Token's "aud" and "iss" claims (https://tools.ietf.org/html/rfc7519).
    * If successful it stores the principal in requestContext for authorization.
    * If not context fails with statusCode 401.
    */
  override def handle(context: RoutingContext): Unit = {
    val user: Option[User] = context.user()

    val principal = user match {
      case Some(u) => KeycloakHelper.accessToken(u.principal())
      case _ =>
        val exception = AuthenticationException("No user in context")
        logger.error(exception.getMessage)
        context.fail(exception.statusCode, exception)
        throw exception
    }

    checkAudience(context, principal)
    checkIssuer(context, principal)

    requestContext.principal = principal

    logger.debug("Token Validation OK")
    context.next()
  }

  private def checkAudience(context: RoutingContext, principal: JsonObject): Unit = {
    val audiences: Seq[String] = principal.getValue("aud") match {
      case s: String => Seq(s)
      case o: JsonArray => o.asScala.map({ case item: String => item }).toSeq
    }

    if (!audiences.contains(getAudience)) {
      val exception = AuthenticationException(
        s"Audiences $audiences of the request doesn't contain configured service audience (resource) '$getAudience'")
      logger.error(exception.getMessage)
      context.fail(exception.statusCode, exception)
      throw exception
    }
  }

  private def checkIssuer(context: RoutingContext, principal: JsonObject): Unit = {
    val issuer: String = principal.getString("iss", "_invalid_")

    if (issuer != getIssuer) {
      val exception = AuthenticationException(
        s"Issuer '$issuer' of the request doesn't match to configured service issuer '$getIssuer'")
      logger.error(exception.getMessage)
      context.fail(exception.statusCode, exception)
      throw exception
    }
  }

  def getAudience: String = tableauxConfig.authConfig.getString("resource")
  def getIssuer: String = tableauxConfig.authConfig.getString("issuer")
}
