package com.campudus.tableaux.database

import scala.concurrent.Future
import org.vertx.scala.core.json.{ JsonArray, Json }
import com.campudus.tableaux.database.TableStructure._
import org.vertx.java.core.json.JsonObject
import com.campudus.tableaux.NotFoundInDatabaseException
import com.campudus.tableaux.database.ResultChecker._

object TableStructure {
  type IdType = Long
  type Ordering = Long
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
    id <- Future.apply(insertNotNull(result)(0).get[Long](0))
    (t, _) <- t.query(s"CREATE TABLE user_table_$id (id BIGSERIAL, PRIMARY KEY (id))", Json.arr())
    (t, _) <- t.query(s"CREATE SEQUENCE system_columns_column_id_table_$id", Json.arr())
    _ <- t.commit()
  } yield id

  def get(tableId: IdType): Future[(IdType, String)] = {
    connection.singleQuery("SELECT table_id, user_table_name FROM system_table WHERE table_id = ?", Json.arr(tableId))
  } map { r =>
    val json = selectNotNull(r)(0)
    (json.get[IdType](0), json.get[String](1))
  }

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

  def insert(tableId: IdType, dbType: TableauxDbType, name: String): Future[(IdType, Ordering)] = for {
    t <- connection.begin()
    (t, result) <- t.query(s"""
                     |INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering)
                     |  VALUES (?, nextval('system_columns_column_id_table_$tableId'), ?, ?, currval('system_columns_column_id_table_$tableId'))
                     |  RETURNING column_id, ordering""".stripMargin,
      Json.arr(tableId, dbType.toString, name))
    result <- Future.successful(insertNotNull(result)(0))
    (t, _) <- t.query(s"ALTER TABLE user_table_$tableId ADD column_${result.get[IdType](0)} $dbType", Json.arr())
    _ <- t.commit()
  } yield (result.get[IdType](0), result.get[Ordering](1))

  def insertLink(tableId: IdType, name: String, fromColumnId: IdType, toTableId: IdType, toColumnId: IdType): Future[(IdType, Ordering)] = for {
    t <- connection.begin()
    (t, result) <- t.query(s"""INSERT INTO system_link_table (table_id_1, table_id_2, column_id_1, column_id_2) VALUES (?, ?, ?, ?) RETURNING link_id""".stripMargin,
      Json.arr(tableId, toTableId, fromColumnId, toColumnId))
    linkId <- Future.successful { insertNotNull(result)(0).get[IdType](0) }
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
                    |) RETURNING column_id, ordering""".stripMargin,
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
  } yield {
    val json = insertNotNull(result)(0)
    (json.get[IdType](0), json.get[Ordering](1))
  }

  def get(tableId: IdType, columnId: IdType): Future[(IdType, String, TableauxDbType, Ordering)] = for {
    result <- connection.singleQuery("""
                              |SELECT column_id, user_column_name, column_type, ordering
                              |  FROM system_columns
                              |  WHERE table_id = ? AND column_id = ?
                              |  ORDER BY column_id""".stripMargin, Json.arr(tableId, columnId))
  } yield {
    val json = selectNotNull(result)(0)
    (json.get[IdType](0), json.get[String](1), Mapper.getDatabaseType(json.get[String](2)), json.get[Ordering](3))
  }

  def getAll(tableId: IdType): Future[Seq[(IdType, String, TableauxDbType, Ordering)]] = {
    connection.singleQuery("SELECT column_id, user_column_name, column_type, ordering FROM system_columns WHERE table_id = ? ORDER BY column_id", Json.arr(tableId))
  } map { getSeqOfJsonArray(_) map { arr => (arr.get[IdType](0), arr.get[String](1), Mapper.getDatabaseType(arr.get[String](2)), arr.get[Ordering](3)) } }

  def getToColumn(tableId: IdType, columnId: IdType): Future[(IdType, IdType)] = for {
    result <- connection.singleQuery("""
                              |SELECT table_id, column_id
                              |  FROM system_columns
                              |  WHERE table_id != ? AND column_id != ? 
                              |  ORDER BY column_id""".stripMargin, Json.arr(tableId, columnId))
  } yield {
    val json = selectNotNull(result)(0)
    (json.get[IdType](0), json.get[IdType](1))
  }

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
  } map { insertNotNull(_)(0).get[IdType](0) }

  def createFull(tableId: IdType, values: Seq[(IdType, _)]): Future[IdType] = {
    val qm = values.foldLeft(Seq[String]())((s, _) => s :+ "?").mkString(", ")
    val columns = values.foldLeft(Seq[String]())((s, tup) => s :+ s"column_${tup._1}").mkString(", ")
    val v = values.foldLeft(Seq[Any]())((s, tup) => s :+ tup._2)
    connection.singleQuery(s"INSERT INTO user_table_$tableId ($columns) VALUES ($qm) RETURNING id", Json.arr(v: _*))
  } map { insertNotNull(_)(0).get[IdType](0) }

  def get(tableId: IdType, rowId: IdType): Future[(IdType, Seq[AnyRef])] = {
    connection.singleQuery(s"SELECT * FROM user_table_$tableId WHERE id = ?", Json.arr(rowId))
  } map { x =>
    val seq = jsonArrayToSeq(selectNotNull(x)(0))
    (seq(0), seq.drop(1))
  }

  def getAll(tableId: IdType): Future[Seq[(IdType, Seq[AnyRef])]] = {
    connection.singleQuery(s"SELECT * FROM user_table_$tableId ORDER BY id", Json.arr())
  } map { x =>
    val seq = getSeqOfJsonArray(x) map { jsonArrayToSeq(_) }
    seq map { s => (s(0), s.drop(1)) }
  }

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
    linkId <- Future.successful(selectNotNull(result)(0).get[IdType](0))
    (t, _) <- t.query(s"INSERT INTO link_table_$linkId VALUES (?, ?)", Json.arr(values._1, values._2))
    _ <- t.commit()
  } yield ()

  def getValue(tableId: IdType, columnId: IdType, rowId: IdType): Future[AnyRef] = {
    connection.singleQuery(s"SELECT column_$columnId FROM user_table_$tableId WHERE id = ?", Json.arr(rowId))
  } map { selectNotNull(_)(0).get[AnyRef](0) }

  def getLinkValues(tableId: IdType, linkColumnId: IdType, rowId: IdType, toTableId: IdType, toColumnId: IdType): Future[AnyRef] = for {
    t <- connection.begin()
    (t, result) <- t.query("SELECT link_id FROM system_columns WHERE table_id = ? AND column_id = ?", Json.arr(tableId, linkColumnId))
    linkId <- Future.successful(selectNotNull(result)(0).get[IdType](0))
    (t, result) <- t.query("SELECT table_id_1, table_id_2, column_id_1, column_id_2 FROM system_link_table WHERE link_id = ?", Json.arr(linkId))
    (id1, id2) <- Future.successful {
      val res = selectNotNull(result)(0)
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
  } yield selectNotNull(result)(0).get[AnyRef](0)
}