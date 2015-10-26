package io.vertx.scala

import com.typesafe.scalalogging.LazyLogging
import io.vertx.core.{Vertx, AbstractVerticle}

import scala.concurrent.Promise
import scala.util.{Failure, Success}

abstract class ScalaVerticle extends AbstractVerticle with VertxExecutionContext with LazyLogging {

  type VertxFuture[A] = io.vertx.core.Future[A]

  /**
   * A reference to the Vert.x runtime.
   * @return A reference to a Vertx.
   */
  lazy val _vertx: Vertx = vertx

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

  override final def stop(future: VertxFuture[Void]): Unit = {
    val p = Promise[Unit]()

    stop(p)

    p.future onComplete {
      case Success(_) =>
        logger.info("ScalaVerticle successfully stopped.")
        future.complete()
      case Failure(ex) =>
        logger.error("ScalaVerticle failed to stopped.", ex)
        future.fail(ex)
    }
  }

  def start(promise: Promise[Unit]): Unit

  def stop(promise: Promise[Unit]): Unit
}