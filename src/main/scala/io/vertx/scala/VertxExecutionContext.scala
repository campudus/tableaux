package io.vertx.scala

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.LazyLogging
import io.vertx.core.{Context, Vertx}
import io.vertx.scala.FunctionConverters._

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

trait VertxExecutionContext extends LazyLogging {

  val _vertx: Vertx

  implicit lazy val executionContext = VertxEventLoopExecutionContext(_vertx, { e => logger.error("Error executing Future in VertxExecutionContext", e) })
}

object VertxEventLoopExecutionContext {
  def apply(vertx: Vertx, errorHandler: (Throwable) => Unit): VertxEventLoopExecutionContext = {
    new VertxEventLoopExecutionContext(vertx, errorHandler)
  }
}

class VertxEventLoopExecutionContext(val vertx: Vertx, val errorHandler: (Throwable) => Unit) extends ExecutionContext with LazyLogging {

  val context: Context = Option(Vertx.currentContext()).getOrElse(vertx.getOrCreateContext)

  val integer = new AtomicInteger(0)

  override def execute(runnable: Runnable): Unit = {
    val _vertx = vertx
    val timerId = _vertx.setTimer(10000, { d: java.lang.Long => logger.error(s"Execution on EventLoop took longer than expected (more than 10000ms).") })

    if (context == Vertx.currentContext()) {
      try {
        runnable.run()
      } catch {
        case NonFatal(e) =>
          reportFailure(e)
      } finally {
        _vertx.cancelTimer(timerId)
      }
    } else {
      context.runOnContext({ _: Void =>
        try {
          runnable.run()
        } catch {
          case NonFatal(e) =>
            reportFailure(e)
        } finally {
          _vertx.cancelTimer(timerId)
        }
      })
    }
  }

  override def reportFailure(cause: Throwable): Unit = errorHandler(cause)
}