package com.campudus.tableaux

import com.typesafe.scalalogging.LazyLogging
import io.vertx.core.{DeploymentOptions, Vertx}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.FunctionConverters._
import org.junit.runner.RunWith
import org.junit.{Before, Test}
import org.vertx.scala.core.json._

import scala.util.{Failure, Success, Try}

@RunWith(classOf[VertxUnitRunner])
class StarterTest extends LazyLogging with TestAssertionHelper with JsonCompatible {

  val verticle = new Starter
  implicit lazy val executionContext = verticle.executionContext

  val vertx: Vertx = Vertx.vertx()
  private var deploymentId: String = ""

  @Before
  def before(context: TestContext) {
    val async = context.async()
    async.complete()
  }

  @Test
  def deployStarterVerticleWithEmptyConfig(implicit c: TestContext): Unit = {
    val async = c.async()

    val options = new DeploymentOptions()
      // will fail because of empty config
      .setConfig(Json.obj())

    val completionHandler = {
      case Success(id) =>
        logger.error(s"Verticle deployed with ID $id but shouldn't.")
        this.deploymentId = id

        c.fail("Verticle deployment should fail.")
      case Failure(e) =>
        logger.info("Verticle couldn't be deployed.", e)
        async.complete()
    }: Try[String] => Unit

    vertx.deployVerticle(verticle, options, completionHandler)
  }

  @Test
  def deployStarterVerticleWithWrongConfig(implicit c: TestContext): Unit = {
    val async = c.async()

    val options = new DeploymentOptions()
      .setConfig(Json.obj(
        // will fail while parsing host
        "host" -> 123,
        "port" -> "123",
        "database" -> Json.obj(
          "username" -> "postgres"
        )
      ))

    val completionHandler = {
      case Success(id) =>
        logger.error(s"Verticle deployed with ID $id but shouldn't.")
        this.deploymentId = id

        c.fail("Verticle deployment should fail.")
      case Failure(e) =>
        logger.info("Verticle couldn't be deployed.", e)
        async.complete()
    }: Try[String] => Unit

    vertx.deployVerticle(verticle, options, completionHandler)
  }
}