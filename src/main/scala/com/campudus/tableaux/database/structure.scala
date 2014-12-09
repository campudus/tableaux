package com.campudus.tableaux.database

import org.vertx.scala.core.FunctionConverters._
import scala.concurrent.{ Future, Promise }
import org.vertx.scala.core.VertxExecutionContext
import TableStructure._
import com.campudus.tableaux.Starter
import org.vertx.scala.core.json.{ Json, JsonObject, JsonArray }
import org.vertx.scala.core.eventbus.Message
import org.vertx.scala.core.Vertx
import org.vertx.scala.platform.Verticle
import org.vertx.scala.core.eventbus.EventBus
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scala.concurrent.ExecutionContext

sealed trait ColumnType {
  type Value

  def columnId: Option[IdType]

  def name: String

  def table: Table

  def setValue(rowId: IdType, value: Value): Future[Unit] = ???

}

sealed trait ValueColumnType extends ColumnType {
  def dbType: String
  def withNewColumnId(colId: IdType): ValueColumnType
}

case class StringColumn(table: Table, columnId: Option[IdType], name: String) extends ValueColumnType {
  type Value = String
  val dbType = "text"
  override def withNewColumnId(colId: IdType) = copy(columnId = Some(colId))
}

case class NumberColumn(table: Table, columnId: Option[IdType], name: String) extends ValueColumnType {
  type Value = Number
  val dbType = "numeric"
  override def withNewColumnId(colId: IdType) = copy(columnId = Some(colId))
}

case class LinkColumn(table: Table, columnId: Option[IdType], to: IdType, name: String) extends ColumnType {
  type Value = Link
}

case class Link(value: Seq[IdType])

case class Table(id: IdType, name: String, columns: Seq[ColumnType]) {
  def getColumn(columnId: IdType)(implicit executionContext: ExecutionContext): Future[ColumnType] = {
    Future.apply(columns.find(_.columnId == columnId).get)
  }
}

object TableStructure {
  val DEFAULT_TIMEOUT = 5000L
  type IdType = Long

  val address = "campudus.asyncdb"
}

class TableStructure(verticle: Verticle) {
  import TableStructure._

  implicit val executionContext = VertxExecutionContext.fromVertxAccess(verticle)

  case class Transaction(msg: Message[JsonObject]) {

    def query(query: String, values: JsonArray): Future[(Transaction, JsonObject)] = {
      val jsonQuery = Json.obj(
        "action" -> "prepared",
        "statement" -> query,
        "values" -> values)

      val p = Promise[(Transaction, JsonObject)]
      msg.replyWithTimeout(jsonQuery, DEFAULT_TIMEOUT, {
        case Success(rep) => p.success((Transaction(rep), rep.body()))
        case Failure(ex) =>
          verticle.logger.error("fail in query", ex)
          p.failure(ex)
      }: Try[Message[JsonObject]] => Unit)
      p.future
    }

    def commit(): Future[Unit] = {
      val p = Promise[Unit]
      msg.replyWithTimeout(Json.obj("action" -> "commit"), DEFAULT_TIMEOUT, {
        case Success(rep) => p.success()
        case Failure(ex) =>
          verticle.logger.error("fail in commit", ex)
          p.failure(ex)
      }: Try[Message[JsonObject]] => Unit)
      p.future
    }

    def rollback(): Future[Unit] = {
      val p = Promise[Unit]
      msg.replyWithTimeout(Json.obj("action" -> "rollback"), DEFAULT_TIMEOUT, {
        case Success(rep) => p.success()
        case Failure(ex) =>
          verticle.logger.error("fail in rollback", ex)
          p.failure(ex)
      }: Try[Message[JsonObject]] => Unit)
      p.future
    }

    def recover(eb: EventBus): PartialFunction[Throwable, Future[Transaction]] = {
      case ex: Throwable => rollback() flatMap (_ => Future.failed[Transaction](ex))
    }
  }

  def deinstall(eb: EventBus): Future[Unit] = for {
    t <- beginTransaction(eb)
    (t, res) <- t.query(s"""DROP SCHEMA public CASCADE""".stripMargin, Json.arr())
    (t, res) <- t.query(s"""CREATE SCHEMA public""".stripMargin, Json.arr())
    _ <- t.commit()
  } yield ()

  def setup(eb: EventBus): Future[Unit] = for {
    t <- beginTransaction(eb)
    (t, res) <- t.query(s"""
                     |CREATE TABLE system_table (
                     |  table_id BIGSERIAL,
                     |  user_table_names VARCHAR(255) NOT NULL,
                     |  PRIMARY KEY(table_id)
                     |)""".stripMargin,
      Json.arr())
    (t, res) <- t.query(s"""
                     |CREATE TABLE system_columns(
                     |  table_id BIGINT,
                     |  column_id BIGINT,
                     |  user_column_name VARCHAR(255) NOT NULL,
                     |  ordering BIGINT NOT NULL,
                     |  link_id BIGINT,
                     |
                     |  PRIMARY KEY(table_id, column_id),
                     |  FOREIGN KEY(table_id)
                     |  REFERENCES system_table(table_id)
                     |  ON DELETE CASCADE
                     |)""".stripMargin,
      Json.arr())
    (t, res) <- t.query(s"""
                     |CREATE TABLE system_link_table(
                     |  link_id BIGSERIAL,
                     |  table_id_1 BIGINT,
                     |  table_id_2 BIGINT,
                     |  column_id_1 BIGINT,
                     |  column_id_2 BIGINT,
                     |
                     |  PRIMARY KEY(link_id),
                     |  FOREIGN KEY(table_id_1, column_id_1)
                     |  REFERENCES system_columns(table_id, column_id)
                     |  ON DELETE CASCADE,
                     |  FOREIGN KEY(table_id_2, column_id_2)
                     |  REFERENCES system_columns(table_id, column_id)
                     |  ON DELETE CASCADE
                     |)""".stripMargin,
      Json.arr())
    (t, res) <- t.query(s"""
                     |ALTER TABLE system_columns
                     |  ADD FOREIGN KEY(link_id)
                     |  REFERENCES system_link_table(link_id)
                     |  ON DELETE CASCADE""".stripMargin,
      Json.arr())
    _ <- t.commit()
  } yield ()

  def createTable(eb: EventBus, name: String): Future[IdType] = for {
    t <- beginTransaction(eb)
    (t, result) <- t.query("INSERT INTO system_table (user_table_names) VALUES (?) RETURNING table_id", Json.arr(name))
    id <- {
      verticle.logger.info(s"result = ${result.encodePrettily()}")
      Future.successful(result.getArray("results").get[JsonArray](0).get[Long](0))
    }
    (t, _) <- t.query(s"CREATE TABLE user_table_$id (id BIGSERIAL, PRIMARY KEY (id))", Json.arr())
    (t, _) <- t.query(s"CREATE SEQUENCE system_columns_column_id_table_$id", Json.arr())
    _ <- t.commit()
  } yield id

  def getTable(eb: EventBus, tableId: IdType): Future[String] = for {
    t <- beginTransaction(eb)
    (t, result) <- t.query("SELECT table_id, user_table_names FROM system_table WHERE table_id = ?", Json.arr(tableId))
    n <- Future.successful(result.getArray("results").get[JsonArray](0).get[String](1))
    _ <- t.commit()
  } yield n

  def insertColumn[T <: ValueColumnType](eb: EventBus, tableId: IdType, column: T): Future[Long] = for {
    t <- beginTransaction(eb)
    (t, result) <- t.query(s"""
                     |INSERT INTO system_columns 
                     |  VALUES (?, nextval('system_columns_column_id_table_$tableId'), ?, currval('system_columns_column_id_table_$tableId'))
                     |  RETURNING column_id""".stripMargin,
      Json.arr(tableId, column.name))
    id <- Future.successful(result.getArray("results").get[JsonArray](0).get[Long](0))
    (t, _) <- t.query(s"ALTER TABLE user_table_$tableId ADD column_$id ${column.dbType})", Json.arr())
    _ <- t.commit()
  } yield id

  def beginTransaction(eb: EventBus): Future[Transaction] = {
    val p = Promise[Transaction]
    eb.sendWithTimeout(address, Json.obj("action" -> "begin"), DEFAULT_TIMEOUT, {
      case Success(rep) =>
        p.success(Transaction(rep))
      case Failure(ex) =>
        verticle.logger.error("fail in query", ex)
        p.failure(ex)
    }: Try[Message[JsonObject]] => Unit)
    p.future
  }

}

class Tableaux(verticle: Verticle) {
  implicit val executionContext = VertxExecutionContext.fromVertxAccess(verticle)

  val vertx = verticle.vertx
  val tables = new TableStructure(verticle)

  def create(name: String): Future[Table] = for {
    id <- tables.createTable(vertx.eventBus, name)
  } yield new Table(id, name, List())

  def delete(id: IdType): Future[Unit] = ???

  def addColumn[T <: ValueColumnType](tableId: IdType, name: String, columnType: String): Future[T] = for {
    table <- getTable(tableId)
    column <- Future.successful {
      columnType match {
        case "text"    => StringColumn(table, None, name)
        case "numeric" => NumberColumn(table, None, name)
      }
    }
    id <- tables.insertColumn(vertx.eventBus, table.id, column)
  } yield {
    column.withNewColumnId(id)
  }.asInstanceOf[T]

  def insertValue[T <: ColumnType](tableId: IdType, columnId: IdType, rowId: IdType, value: T): Future[Unit] = for {
    table <- getTable(tableId)
    column <- table.getColumn(columnId)
    v: column.Value <- Future.apply(value.asInstanceOf[column.Value])
    _ <- column.setValue(rowId, v)
  } yield ()

  def getTable(tableId: IdType): Future[Table] = for {
    name <- tables.getTable(vertx.eventBus, tableId)
  } yield Table(tableId, name, List())

  def getColumn(columnId: IdType): Future[ColumnType] = ???

}
