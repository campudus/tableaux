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
    if (x != null) OkArg(x.asInstanceOf[A]) else FailArg(InvalidJsonException(s"Warning: $name is null", "null"))
  } catch {
    case npe: NullPointerException => FailArg(InvalidJsonException(s"Warning: $name is null", "null"))
    case cce: ClassCastException => FailArg(InvalidJsonException(s"Warning: $name should be another type. Error: ${cce.getMessage}", "invalid"))
  }

  def greaterThan(x: Long, than: Long, name: String): ArgumentCheck[Long] = {
    if (x > than)
      OkArg(x)
    else
      FailArg(InvalidJsonException(s"Argument $name ($x) is less than $than.", "invalid"))
  }

  def greaterZero(x: Long): ArgumentCheck[Long] = if (x > 0) OkArg(x) else FailArg(InvalidJsonException(s"Argument $x is not greater than zero", "invalid"))

  def nonEmpty[A](seq: Seq[A], name: String): ArgumentCheck[Seq[A]] = {
    if (seq.nonEmpty) OkArg(seq) else FailArg(InvalidJsonException(s"Warning: $name is empty.", "empty"))
  }

  def isDefined[A](option: Option[A], name: String): ArgumentCheck[A] = {
    option.isDefined match {
      case true => OkArg(option.get)
      case false => FailArg(ParamNotFoundException(s"query parameter $name not found"))
    }
  }

  def isDefined(options: Seq[Option[_]], name: String = ""): ArgumentCheck[Unit] = {
    val empty = !options.exists({ o => o.isDefined })
    empty match {
      case true => FailArg(InvalidRequestException(s"Non of these options has a value. ($name)"))
      case false => OkArg(())
    }
  }

  def oneOf[A](x: => A, list: List[A], name: String): ArgumentCheck[A] = {
    if (list.contains(x)) {
      OkArg(x)
    } else {
      FailArg(InvalidRequestException(s"'$name' value needs to be one of ${list.mkString("'", "', '", "'")}."))
    }
  }

  def hasArray(field: String, json: JsonObject): ArgumentCheck[JsonArray] = notNull(json.getJsonArray(field), field)

  def hasNumber(field: String, json: JsonObject): ArgumentCheck[Number] = notNull(json.getValue(field).asInstanceOf[Number], field)

  def hasLong(field: String, json: JsonObject): ArgumentCheck[Long] = notNull(json.getLong(field), field).map(_.longValue())

  def hasString(field: String, json: JsonObject): ArgumentCheck[String] = notNull(json.getString(field), field)

  def tryCast[A](elem: Any): ArgumentCheck[A] = {
    tryMap((x: Any) => x.asInstanceOf[A], InvalidJsonException(s"Warning: $elem should not be ${elem.getClass}", "invalid"))(elem)
  }

  def checkSameLengthsAndZip[A, B](list1: Seq[A], list2: Seq[B]): ArgumentCheck[Seq[(A, B)]] = {
    if (list1.size == list2.size) {
      OkArg(list1.zip(list2))
    } else {
      FailArg(NotEnoughArgumentsException("lists are not equally sized"))
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

  def checked[A](checkedArgument: ArgumentCheck[A]): A = checkedArgument.get
}
