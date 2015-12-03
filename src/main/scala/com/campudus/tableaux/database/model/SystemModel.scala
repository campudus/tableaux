package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.domain.{DomainObject, EmptyObject}
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.Json

import scala.concurrent.Future
import scala.util.Try

object SystemModel {
  val VERSION = 1

  def apply(connection: DatabaseConnection): SystemModel = {
    new SystemModel(connection)
  }
}

class SystemModel(override protected[this] val connection: DatabaseConnection) extends DatabaseQuery {

  def reset(): Future[DomainObject] = {
    for {
      _ <- deinstall()
      _ <- setup()
    } yield EmptyObject()
  }

  def deinstall(): Future[Unit] = for {
    t <- connection.begin()
    (t, _) <- t.query("DROP SCHEMA public CASCADE")
    (t, _) <- t.query("CREATE SCHEMA public")
    _ <- t.commit()
  } yield ()

  private def saveVersion(t: connection.Transaction, version: Int): Future[connection.Transaction] = {
    t.query(
      s"""
         |INSERT INTO system_version (version)
         |SELECT ? WHERE NOT EXISTS (SELECT version FROM system_version WHERE version = ?)
       """.stripMargin, Json.arr(version, version)) map {
      case (t, _) => t
    }
  }

  def retrieveVersion(): Future[Long] = {
    for {
      t <- connection.begin()
      (t, version) <- retrieveVersion(t)
      _ <- t.commit()
    } yield version
  }

  def retrieveVersion(t: connection.Transaction): Future[(connection.Transaction, Long)] = {
    for {
      (t, _) <- t.query(
        s"""
           |CREATE TABLE IF NOT EXISTS system_version(
           |version BIGINT NOT NULL,
           |updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
           |PRIMARY KEY(version)
           |)
       """.stripMargin)

      (t, version) <- {
        t.query("SELECT max_version FROM (SELECT MAX(version) AS max_version FROM system_version) sub WHERE max_version IS NOT NULL") map {
          case (t, result) =>
            logger.info(result.encode())
            val version = Try(jsonArrayToSeq(selectNotNull(result).head).head).getOrElse(-1L)
            (t, version)
        }
      }
    } yield (t, version)
  }

  def setup(): Future[Unit] = for {
    t <- connection.begin()
    (t, version) <- retrieveVersion(t)

    (t, _) <- {
      version match {
        case SystemModel.VERSION | _ =>
          for {
            t <- setupVersion1(t)
            t <- setupVersion2(t)
            t <- setupVersion3(t)
          } yield t
        case 1L =>
          for {
            t <- setupVersion1(t)
            t <- setupVersion2(t)
            t <- setupVersion3(t)
          } yield t
      }
    }
  } yield ()

  private def setupVersion1(t: connection.Transaction): Future[connection.Transaction] = for {
    (t, _) <- t.query(
      s"""
         |CREATE TABLE system_table (
         |table_id BIGSERIAL,
         |user_table_name VARCHAR(255) NOT NULL,
         |PRIMARY KEY(table_id)
         |)""".stripMargin)

    (t, _) <- t.query(
      s"""
         |CREATE TABLE system_columns(
         |table_id BIGINT,
         |column_id BIGINT,
         |column_type VARCHAR(255) NOT NULL,
         |user_column_name VARCHAR(255) NOT NULL,
         |ordering BIGINT NOT NULL,
         |link_id BIGINT,
         |multilanguage BOOLEAN,
         |
         |PRIMARY KEY(table_id, column_id),
         |FOREIGN KEY(table_id)
         |REFERENCES system_table(table_id)
         |ON DELETE CASCADE
         |)""".stripMargin)

    (t, _) <- t.query(
      s"""
         |CREATE TABLE system_link_table(
         |link_id BIGSERIAL,
         |table_id_1 BIGINT,
         |table_id_2 BIGINT,
         |column_id_1 BIGINT,
         |column_id_2 BIGINT,
         |
         |PRIMARY KEY(link_id),
         |FOREIGN KEY(table_id_1, column_id_1)
         |REFERENCES system_columns(table_id, column_id)
         |ON DELETE CASCADE,
         |FOREIGN KEY(table_id_2, column_id_2)
         |REFERENCES system_columns(table_id, column_id)
         |ON DELETE CASCADE
         |)""".stripMargin)

    (t, _) <- t.query(
      s"""
         |CREATE TABLE folder(
         |id BIGSERIAL NOT NULL,
         |name VARCHAR(255) NOT NULL,
         |description VARCHAR(255) NOT NULL,
         |idparent BIGINT NULL,
         |created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
         |updated_at TIMESTAMP WITHOUT TIME ZONE,
         |
         |PRIMARY KEY(id),
         |FOREIGN KEY(idparent)
         |REFERENCES folder(id) MATCH SIMPLE
         |ON UPDATE NO ACTION ON DELETE NO ACTION
         |)""".stripMargin)

    t <- saveVersion(t, 1)
  } yield t


  private def setupVersion2(t: connection.Transaction): Future[connection.Transaction] = for {
    (t, _) <- t.query(
      s"""
         |CREATE TABLE file(
         |uuid UUID NOT NULL,
         |idfolder BIGINT NULL,
         |tmp BOOLEAN NOT NULL DEFAULT TRUE,

         |created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
         |updated_at TIMESTAMP WITHOUT TIME ZONE,
         |
         |PRIMARY KEY(uuid),
         |
         |FOREIGN KEY(idfolder) REFERENCES folder(id) MATCH SIMPLE
         |ON UPDATE NO ACTION ON DELETE NO ACTION
         |)""".stripMargin)

    (t, _) <- t.query(
      s"""
         |CREATE TABLE file_lang(
         |uuid UUID NOT NULL,
         |langtag VARCHAR(50) NOT NULL,
         |
         |title VARCHAR(255) NULL,
         |description VARCHAR(255) NULL,
         |
         |internal_name VARCHAR(255) NULL UNIQUE,
         |external_name VARCHAR(255) NULL,
         |
         |mime_type VARCHAR(255) NULL,
         |PRIMARY KEY(uuid, langtag),
         |
         |FOREIGN KEY(uuid) REFERENCES file(uuid)
         |ON DELETE CASCADE
         |)""".stripMargin)

    t <- saveVersion(t, 2)
  } yield t

  private def setupVersion3(t: connection.Transaction): Future[connection.Transaction] = for {
    (t, _) <- t.query(
      s"""
         |CREATE TABLE system_attachment(
         |table_id BIGINT NOT NULL,
         |column_id BIGINT NOT NULL,
         |row_id BIGINT NOT NULL,
         |attachment_uuid UUID NOT NULL,
         |
         |ordering BIGINT NOT NULL,
         |
         |created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
         |updated_at TIMESTAMP WITHOUT TIME ZONE,
         |
         |PRIMARY KEY(table_id, column_id, row_id, attachment_uuid),
         |
         |FOREIGN KEY(table_id, column_id)
         |REFERENCES system_columns(table_id, column_id)
         |ON DELETE CASCADE,
         |
         |FOREIGN KEY(attachment_uuid)
         |REFERENCES file(uuid) MATCH SIMPLE
         |ON DELETE CASCADE
         |)""".stripMargin)

    (t, _) <- t.query(
      s"""
         |ALTER TABLE system_columns
         |ADD FOREIGN KEY(link_id)
         |REFERENCES system_link_table(link_id)
         |ON DELETE CASCADE""".stripMargin)

    t <- saveVersion(t, 3)
  } yield t
}