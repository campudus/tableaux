package com.campudus.tableaux.api

import com.campudus.tableaux.Starter
import com.campudus.tableaux.helper.Json
import com.campudus.tableaux.testtools.TestAssertionHelper

import io.vertx.core.{DeploymentOptions, Vertx}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.{ScalaVerticle, VertxExecutionContext, *}
import io.vertx.lang.scala.json._

import scala.util.{Failure, Success, Try}

import com.typesafe.scalalogging.LazyLogging
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class StarterTest extends LazyLogging with TestAssertionHelper {

  val vertx: Vertx = Vertx.vertx()

  implicit lazy val executionContext: VertxExecutionContext = VertxExecutionContext(vertx, vertx.getOrCreateContext())

  @Test
  def deployStarterVerticleWithEmptyConfig(implicit c: TestContext): Unit = {
    val async = c.async()

    val options = DeploymentOptions()
      // will fail because of empty config
      .setConfig(Json.obj())

    val completionHandler: Try[String] => Unit = {
      case Success(id) =>
        logger.error(s"Verticle deployed with ID $id but shouldn't.")

        c.fail("Verticle deployment should fail.")
      case Failure(e) =>
        logger.info("Verticle couldn't be deployed.", e)
        async.complete()
    }

    vertx
      .deployVerticle(ScalaVerticle.nameForVerticle[Starter](), options)
      .asScala
      .onComplete(completionHandler)
  }

  @Test
  def deployStarterVerticleWithWrongConfig(implicit c: TestContext): Unit = {
    val async = c.async()

    val options = DeploymentOptions()
      .setConfig(
        Json.obj(
          // will fail while parsing host
          "host" -> 123,
          "port" -> "123",
          "database" -> Json.obj(
            "username" -> "postgres"
          )
        )
      )

    val completionHandler: Try[String] => Unit = {
      case Success(id) =>
        logger.error(s"Verticle deployed with ID $id but shouldn't.")

        c.fail("Verticle deployment should fail.")
      case Failure(e) =>
        logger.info("Verticle couldn't be deployed.", e)
        async.complete()
    }

    vertx
      .deployVerticle(ScalaVerticle.nameForVerticle[Starter](), options)
      .asScala
      .onComplete(completionHandler)
  }
}
