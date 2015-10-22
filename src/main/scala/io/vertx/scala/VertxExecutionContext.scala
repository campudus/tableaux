package io.vertx.scala

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.LazyLogging
import io.vertx.core.Vertx
import io.vertx.core.logging.{Logger, LoggerFactory}
import io.vertx.scala.FunctionConverters._

import scala.concurrent.ExecutionContext

trait VertxExecutionContext {

  private def logger: Logger = LoggerFactory.getLogger(classOf[VertxExecutionContext])

  val _vertx: Vertx

  implicit lazy val executionContext = VertxEventLoopExecutionContext(_vertx, { e => logger.error("Error executing Future in VertxExecutionContext", e) })
}

object VertxEventLoopExecutionContext {
  def apply(vertx: Vertx, errorHandler: (Throwable) => Unit): VertxEventLoopExecutionContext = {
    new VertxEventLoopExecutionContext(vertx, errorHandler)
  }
}

class VertxEventLoopExecutionContext(val vertx: Vertx, val errorHandler: (Throwable) => Unit) extends ExecutionContext with LazyLogging{

  val context = Option(Vertx.currentContext()).getOrElse(vertx.getOrCreateContext)

  val integer = new AtomicInteger(0)

  override def execute(runnable: Runnable): Unit = {
    val random = integer.getAndIncrement()

    val _vertx = vertx
    val timerId = _vertx.setTimer(10000, { d: java.lang.Long => logger.error(s"$random exceeded the delay")})

    if (context == Vertx.currentContext()) {
      try {
        //logger.info(s"Before runnable $random")
        runnable.run()
        //logger.info(s"After runnable $random")
        _vertx.cancelTimer(timerId)
      } catch {
        case e: Throwable =>
          reportFailure(e)
      }
    } else {
      context.runOnContext({ _: Void =>
        try {
          logger.info(s"Before onContext runnable $random")
          runnable.run()
          logger.info(s"After onContext runnable $random")
          _vertx.cancelTimer(timerId)
        } catch {
          case e: Throwable =>
            reportFailure(e)
        }
      })
    }
  }

  override def reportFailure(cause: Throwable): Unit = errorHandler(cause)
}