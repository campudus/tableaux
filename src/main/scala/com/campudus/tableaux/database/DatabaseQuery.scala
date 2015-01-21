package com.campudus.tableaux.database

import scala.concurrent.Future
import org.vertx.scala.core.json.{ JsonArray, Json }
import com.campudus.tableaux.database.TableStructure._
import org.vertx.java.core.json.JsonObject
import com.campudus.tableaux.NotFoundInDatabaseException
import com.campudus.tableaux.database.ResultChecker._

object TableStructure {
  type IdType = Long
}

class SystemStructure(connection: DatabaseConnection) {
  implicit val executionContext = connection.executionContext

  def deinstall(): Future[Unit] = for {
    t <- connection.begin()
    (t, _) <- t.query("DROP SCHEMA public CASCADE", Json.arr())
    (t, _) <- t.query("CREATE SCHEMA public", Json.arr())
    _ <- t.commit()
  } yield ()

  def setup(): Future[Unit] = for {
    t <- connection.begin()
    (t, _) <- t.query(s"""
                     |CREATE TABLE system_table (
                     |  table_id BIGSERIAL,
                     |  user_table_name VARCHAR(255) NOT NULL,
                     |  PRIMARY KEY(table_id)
                     |)""".stripMargin,
      Json.arr())
    (t, _) <- t.query(s"""
                     |CREATE TABLE system_columns(
                     |  table_id BIGINT,
                     |  column_id BIGINT,
                     |  column_type VARCHAR(255) NOT NULL,
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
    (t, _) <- t.query(s"""
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
    (t, _) <- t.query(s"""
                     |ALTER TABLE system_columns
                     |  ADD FOREIGN KEY(link_id)
                     |  REFERENCES system_link_table(link_id)
                     |  ON DELETE CASCADE""".stripMargin,
      Json.arr())
    _ <- t.commit()
  } yield ()
}

class TableStructure(connection: DatabaseConnection) {
  implicit val executionContext = connection.executionContext

  def create(name: String): Future[IdType] = for {
    t <- connection.begin()
    (t, result) <- t.query("INSERT INTO system_table (user_table_name) VALUES (?) RETURNING table_id", Json.arr(name))
    id <- Future.successful(insertNotNull(result).get[JsonArray](0).get[Long](0))
    (t, _) <- t.query(s"CREATE TABLE user_table_$id (id BIGSERIAL, PRIMARY KEY (id))", Json.arr())
    (t, _) <- t.query(s"CREATE SEQUENCE system_columns_column_id_table_$id", Json.arr())
    _ <- t.commit()
  } yield id

  def get(tableId: IdType): Future[JsonArray] = {
    connection.singleQuery("SELECT table_id, user_table_name FROM system_table WHERE table_id = ?", Json.arr(tableId))
  } map { selectNotNull(_).get[JsonArray](0) }

  def delete(tableId: IdType): Future[Unit] = for {
    t <- connection.begin()
    (t, _) <- t.query(s"DROP TABLE IF EXISTS user_table_$tableId", Json.arr())
    (t, result) <- t.query("DELETE FROM system_table WHERE table_id = ?", Json.arr(tableId))
    _ <- Future.apply(deleteNotNull(result)) recoverWith { t.rollbackAndFail() }
    (t, _) <- t.query(s"DROP SEQUENCE system_columns_column_id_table_$tableId", Json.arr())
    _ <- t.commit()
  } yield ()
}

class ColumnStructure(connection: DatabaseConnection) {
  implicit val executionContext = connection.executionContext

  def insert(tableId: IdType, dbType: String, name: String): Future[IdType] = for {
    t <- connection.begin()
    (t, result) <- t.query(s"""
                     |INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering)
                     |  VALUES (?, nextval('system_columns_column_id_table_$tableId'), ?, ?, currval('system_columns_column_id_table_$tableId'))
                     |  RETURNING column_id""".stripMargin,
      Json.arr(tableId, dbType, name))
    id <- Future.successful(insertNotNull(result).get[JsonArray](0).get[Long](0))
    (t, _) <- t.query(s"ALTER TABLE user_table_$tableId ADD column_$id $dbType", Json.arr())
    _ <- t.commit()
  } yield id

  def insertLink(tableId: IdType, name: String, fromColumnId: IdType, toTableId: IdType, toColumnId: IdType): Future[IdType] = for {
    t <- connection.begin()
    (t, result) <- t.query(s"""INSERT INTO system_link_table (table_id_1, table_id_2, column_id_1, column_id_2) VALUES (?, ?, ?, ?) RETURNING link_id""".stripMargin,
      Json.arr(tableId, toTableId, fromColumnId, toColumnId))
    linkId <- Future.successful { insertNotNull(result).get[JsonArray](0).get[Long](0) }
    (t, _) <- t.query(s"""
                    |INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering, link_id) VALUES (
                    |  ?, 
                    |  nextval('system_columns_column_id_table_$toTableId'), 
                    |  'link',
                    |  ?, 
                    |  currval('system_columns_column_id_table_$toTableId'), 
                    |  ?)""".stripMargin,
      Json.arr(toTableId, name, linkId))
    (t, result) <- t.query(s"""
                    |INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering, link_id) VALUES (
                    |  ?, 
                    |  nextval('system_columns_column_id_table_$tableId'), 
                    |  'link',
                    |  ?, 
                    |  currval('system_columns_column_id_table_$tableId'), 
                    |  ?
                    |) RETURNING column_id""".stripMargin,
      Json.arr(tableId, name, linkId))
    (t, _) <- t.query(s"""
                    |CREATE TABLE link_table_$linkId (
                    |  id_1 bigint, 
                    |  id_2 bigint, 
                    |  PRIMARY KEY(id_1, id_2), 
                    |  CONSTRAINT link_table_${linkId}_foreign_1 
                    |    FOREIGN KEY(id_1) 
                    |    REFERENCES user_table_$tableId (id) 
                    |    ON DELETE CASCADE, 
                    |  CONSTRAINT link_table_${linkId}_foreign_2
                    |    FOREIGN KEY(id_2) 
                    |    REFERENCES user_table_$toTableId (id) 
                    |    ON DELETE CASCADE
                    |)""".stripMargin, Json.arr())
    _ <- t.commit()
  } yield insertNotNull(result).get[JsonArray](0).get[Long](0)

  def get(tableId: IdType, columnId: IdType): Future[JsonArray] = for {
    result <- connection.singleQuery("""
                              |SELECT column_id, user_column_name, column_type
                              |  FROM system_columns
                              |  WHERE table_id = ? AND column_id = ?
                              |  ORDER BY column_id""".stripMargin, Json.arr(tableId, columnId))
  } yield selectNotNull(result).get[JsonArray](0)

  def getAll(tableId: IdType): Future[JsonArray] = {
    connection.singleQuery("SELECT column_id, user_column_name, column_type FROM system_columns WHERE table_id = ? ORDER BY column_id", Json.arr(tableId))
  } map { getJsonArray(_) }

  def getToColumn(tableId: IdType, columnId: IdType): Future[JsonArray] = for {
    result <- connection.singleQuery("""
                              |SELECT table_id, column_id
                              |  FROM system_columns
                              |  WHERE table_id != ? AND column_id != ? 
                              |  ORDER BY column_id""".stripMargin, Json.arr(tableId, columnId))
  } yield selectNotNull(result).get[JsonArray](0)

  def delete(tableId: IdType, columnId: IdType): Future[Unit] = for {
    t <- connection.begin()
    (t, _) <- t.query(s"ALTER TABLE user_table_$tableId DROP COLUMN IF EXISTS column_$columnId", Json.arr())
    (t, result) <- t.query("DELETE FROM system_columns WHERE column_id = ? AND table_id = ?", Json.arr(columnId, tableId))
    _ <- Future.apply(deleteNotNull(result)) recoverWith t.rollbackAndFail()
    _ <- t.commit()
  } yield ()
}

class RowStructure(connection: DatabaseConnection) {
  implicit val executionContext = connection.executionContext

  def create(tableId: IdType): Future[IdType] = {
    connection.singleQuery(s"INSERT INTO user_table_$tableId DEFAULT VALUES RETURNING id", Json.arr())
  } map { insertNotNull(_).get[JsonArray](0).get[IdType](0) }

  def get(tableId: IdType, rowId: IdType): Future[JsonArray] = {
    connection.singleQuery(s"SELECT * FROM user_table_$tableId WHERE id = ?", Json.arr(rowId))
  } map { selectNotNull(_) }

  def getAllFromColumn(tableId: IdType, columnId: IdType): Future[JsonArray] = {
    connection.singleQuery(s"SELECT id, column_$columnId FROM user_table_$tableId ORDER BY id", Json.arr())
  } map { getJsonArray(_) }

  def delete(tableId: IdType, rowId: IdType): Future[Unit] = {
    connection.singleQuery(s"DELETE FROM user_table_$tableId  WHERE id = ?", Json.arr(rowId))
  } map { deleteNotNull(_) }
}

class CellStructure(connection: DatabaseConnection) {
  implicit val executionContext = connection.executionContext

  def update[A](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Unit] = {
    connection.singleQuery(s"UPDATE user_table_$tableId SET column_$columnId = ? WHERE id = ?", Json.arr(value, rowId))
  } map { _ => () }

  def updateLink(tableId: IdType, linkColumnId: IdType, values: (IdType, IdType)): Future[Unit] = for {
    t <- connection.begin()
    (t, result) <- t.query("SELECT link_id FROM system_columns WHERE table_id = ? AND column_id = ?", Json.arr(tableId, linkColumnId))
    linkId <- Future.successful(selectNotNull(result).get[JsonArray](0).get[IdType](0))
    (t, _) <- t.query(s"INSERT INTO link_table_$linkId VALUES (?, ?)", Json.arr(values._1, values._2))
    _ <- t.commit()
  } yield ()

  def getValue(tableId: IdType, columnId: IdType, rowId: IdType): Future[JsonArray] = {
    connection.singleQuery(s"SELECT column_$columnId FROM user_table_$tableId WHERE id = ?", Json.arr(rowId))
  } map { selectNotNull(_) }

  def getLinkValues(tableId: IdType, linkColumnId: IdType, rowId: IdType, toTableId: IdType, toColumnId: IdType): Future[JsonArray] = for {
    t <- connection.begin()
    (t, result) <- t.query("SELECT link_id FROM system_columns WHERE table_id = ? AND column_id = ?", Json.arr(tableId, linkColumnId))
    linkId <- Future.successful(selectNotNull(result).get[JsonArray](0).get[IdType](0))
    (t, result) <- t.query("SELECT table_id_1, table_id_2, column_id_1, column_id_2 FROM system_link_table WHERE link_id = ?", Json.arr(linkId))
    (id1, id2) <- Future.successful {
      val res = selectNotNull(result).get[JsonArray](0)
      val linkTo2 = (res.get[IdType](1), res.get[IdType](3))

      if (linkTo2 == (toTableId, toColumnId)) ("id_1", "id_2") else ("id_2", "id_1")
    }
    (t, result) <- t.query(s"""
                     |SELECT STRING_AGG(user_table_$toTableId.column_$toColumnId, ', ') FROM user_table_$tableId 
                     |  LEFT JOIN link_table_$linkId 
                     |    ON user_table_$tableId.id = link_table_${linkId}.$id1
                     |  LEFT JOIN user_table_$toTableId 
                     |    ON user_table_$toTableId.id = link_table_${linkId}.$id2
                     |  WHERE user_table_$tableId.id = ?
                     |  GROUP BY user_table_$tableId.id""".stripMargin, Json.arr(rowId))
    _ <- t.commit()
  } yield selectNotNull(result)
}