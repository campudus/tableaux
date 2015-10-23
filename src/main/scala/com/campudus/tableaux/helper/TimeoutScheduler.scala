package com.campudus.tableaux.helper

import java.util.concurrent.{TimeUnit, TimeoutException}

import io.netty.util.{HashedWheelTimer, Timeout, TimerTask}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}

object TimeoutScheduler {
  val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)

  private def scheduleTimeout[T](timeoutHandler: (Throwable) => Unit, after: Duration) = {
    timer.newTimeout(new TimerTask {
      def run(timeout: Timeout) {
        timeoutHandler(new TimeoutException(s"Operation timed out after ${after.toMillis} millis"))
      }
    }, after.toNanos, TimeUnit.NANOSECONDS)
  }

  implicit class TimeoutFuture[T](fut: Future[T]) {

    import scala.concurrent.duration.DurationInt

    def withTimeout(timeoutMs: Int, name: String)(implicit ec: ExecutionContext): Future[T] = withTimeout(DurationInt(timeoutMs).millis, name)

    def withTimeout(timeout: FiniteDuration, name: String)(implicit ec: ExecutionContext): Future[T] = {
      val promise = Promise[T]()

      val _timeout = TimeoutScheduler.scheduleTimeout({ e => promise.failure(e) }, timeout)

      val combinedFut = Future.firstCompletedOf(List(fut, promise.future))
      fut onComplete { case result => _timeout.cancel() }
      combinedFut
    }

    def withTimeout(timeout: FiniteDuration, timeoutHandler: (Throwable) => Unit)(implicit ec: ExecutionContext): Future[T] = {
      val promise = Promise[T]()

      val _timeout = TimeoutScheduler.scheduleTimeout({ e => promise.failure(e); timeoutHandler(e) }, timeout)

      val combinedFut = Future.firstCompletedOf(List(fut, promise.future))
      fut onComplete { case result => _timeout.cancel() }
      combinedFut
    }
  }
}