package com.campudus.tableaux

import org.vertx.scala.core.json.{JsonArray, JsonObject}

import scala.util.Try

sealed trait ArgumentCheck[A] {
  def map[B](f: A => B): ArgumentCheck[B]

  def flatMap[B](f: A => ArgumentCheck[B]): ArgumentCheck[B]

  def get: A
}

case class OkArg[A](value: A) extends ArgumentCheck[A] {
  def map[B](f: A => B): ArgumentCheck[B] = OkArg(f(value))

  def flatMap[B](f: A => ArgumentCheck[B]): ArgumentCheck[B] = f(value)

  def get: A = value
}

case class FailArg[A](ex: CustomException) extends ArgumentCheck[A] {
  def map[B](f: A => B): ArgumentCheck[B] = FailArg(ex)

  def flatMap[B](f: A => ArgumentCheck[B]): ArgumentCheck[B] = FailArg(ex)

  def get: A = throw ex
}

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
object ArgumentChecker {

  def notNull[A](x: => A, name: String): ArgumentCheck[A] = try {
    if (x != null) OkArg(x) else FailArg(InvalidJsonException(s"Warning: $name is null", "null"))
  } catch {
    case npe: NullPointerException => FailArg(InvalidJsonException(s"Warning: $name is null", "null"))
    case cce: ClassCastException => FailArg(InvalidJsonException(s"Warning: $name should not be ${name.getClass}", "invalid"))
  }

  def greaterZero(x: Long): ArgumentCheck[Long] = if (x > 0) OkArg(x) else FailArg(InvalidJsonException(s"Argument $x is not greater than zero", "invalid"))

  def nonEmpty[A](seq: Seq[A], name: String): ArgumentCheck[Seq[A]] = {
    if (seq.nonEmpty) OkArg(seq) else FailArg(InvalidJsonException(s"Warning: $name is empty.", "empty"))
  }

  def hasArray(arr: String, value: JsonObject): ArgumentCheck[JsonArray] = notNull(value.getArray(arr), arr)

  def hasNumber(num: String, value: JsonObject): ArgumentCheck[Number] = notNull(value.getNumber(num), num)

  def tryCast[A](elem: Any): ArgumentCheck[A] = {
    tryMap((x: Any) => x.asInstanceOf[A], InvalidJsonException(s"Warning: $elem should not be ${elem.getClass}", "invalid"))(elem)
  }

  def checkSameLengthsAndZip[A, B](list1: Seq[A], list2: Seq[B]): ArgumentCheck[Seq[(A, B)]] = {
    if (list1.size == list2.size) {
      OkArg(list1.zip(list2))
    } else {
      FailArg(NotEnoughArgumentsException("lists are not equally sized", "arguments"))
    }
  }

  def sequence[A](argChecks: Seq[ArgumentCheck[A]]): ArgumentCheck[Seq[A]] = {
    argChecks match {
      case Nil => OkArg(Nil)
      case argCheck :: t => argCheck flatMap (a => sequence(t) map (a +: _))
    }
  }

  def tryMap[A, B](tryFn: A => B, ex: => CustomException)(a: A): ArgumentCheck[B] = Try(OkArg(tryFn(a))).getOrElse(FailArg[B](ex))

  def checkArguments(args: ArgumentCheck[_]*): Unit = {
    val failedArgs: Vector[String] = args.zipWithIndex.foldLeft(Vector[String]()) {
      case (errors, (FailArg(ex), idx)) => errors :+ s"($idx) ${ex.message}"
      case (errors, (OkArg(x), idx)) => errors
    }

    if (failedArgs.nonEmpty) throw new IllegalArgumentException(failedArgs.mkString("\n"))
  }

  def checked[A](arg1: ArgumentCheck[A]): A = arg1.get
}
