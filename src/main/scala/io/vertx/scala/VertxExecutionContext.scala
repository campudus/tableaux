package io.vertx.scala

import java.util.Objects

import com.typesafe.scalalogging.LazyLogging
import io.vertx.core.{Context, Vertx}
import io.vertx.scala.FunctionConverters._

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

trait VertxExecutionContext extends LazyLogging {

  val _vertx: Vertx

  /**
    * Must be lazy because declaration of _vertx is lazy
    */
  implicit lazy val executionContext = VertxEventLoopExecutionContext(_vertx)
}

object VertxEventLoopExecutionContext {

  def apply(vertx: Vertx): VertxEventLoopExecutionContext = {
    Objects.requireNonNull(vertx)

    new VertxEventLoopExecutionContext(vertx)
  }
}

class VertxEventLoopExecutionContext private(val vertx: Vertx) extends ExecutionContext with LazyLogging {

  val context: Context = Option(Vertx.currentContext()).getOrElse(vertx.getOrCreateContext())

  override def execute(runnable: Runnable): Unit = {
    val timerId = vertx.setTimer(10000, { d: java.lang.Long => {
      logger.error(s"Execution on EventLoop took longer than expected (more than 10000ms).")
    }
    })

    if (context == Vertx.currentContext()) {
      try {
        runnable.run()
      } catch {
        case NonFatal(e) =>
          reportFailure(e)
      } finally {
        vertx.cancelTimer(timerId)
      }
    } else {
      context.runOnContext({ _: Void => {
        try {
          runnable.run()
        } catch {
          case NonFatal(e) =>
            reportFailure(e)
        } finally {
          vertx.cancelTimer(timerId)
        }
      }
      })
    }
  }

  override def reportFailure(cause: Throwable): Unit = {
    Option(context.exceptionHandler())
      .foreach(_.handle(cause))
  }

  override def prepare(): ExecutionContext = this
}
