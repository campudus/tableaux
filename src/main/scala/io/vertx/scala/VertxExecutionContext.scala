package io.vertx.scala

import io.vertx.core.logging.{LoggerFactory, Logger}

import scala.concurrent.ExecutionContext

trait VertxExecutionContext {

  private def logger: Logger = LoggerFactory.getLogger(classOf[VertxExecutionContext])

  protected final class VertxExecutionContextImpl(logger: => Logger) extends ExecutionContext {
    override def reportFailure(t: Throwable): Unit =
      logger.error("Error executing Future in VertxExecutionContext", t)

    override def execute(runnable: Runnable): Unit =
      runnable.run()
  }

  implicit val executionContext = new VertxExecutionContextImpl(logger)
}