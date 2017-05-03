package com.campudus.tableaux.helper

object Tap {

  import scala.language.implicitConversions

  implicit def anyToTap[A](underlying: A): Tap[A] = new Tap(underlying)
}

class Tap[A](underlying: A) {

  def tap(func: A => Unit): A = {
    func(underlying)
    underlying
  }
}
