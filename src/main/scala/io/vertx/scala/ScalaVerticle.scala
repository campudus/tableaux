package io.vertx.scala

import com.typesafe.scalalogging.LazyLogging
import io.vertx.core.AbstractVerticle
import io.vertx.core.logging.{LoggerFactory, Logger}

import scala.concurrent.Promise
import scala.util.{Failure, Success}

abstract class ScalaVerticle extends AbstractVerticle with VertxExecutionContext with LazyLogging {
  type VertxFuture[A] = io.vertx.core.Future[A]

  override final def start(future: VertxFuture[Void]): Unit = {
    val p = Promise[Unit]()

    start(p)

    p.future onComplete {
      case Success(_) =>
        logger.info("ScalaVerticle successfully started.")
        future.complete()
      case Failure(ex) =>
        logger.error("ScalaVerticle failed to start.", ex)
        future.fail(ex)
    }
  }

  def start(promise: Promise[Unit]): Unit
}