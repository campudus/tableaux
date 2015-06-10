package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

object SystemModel {
  def apply(connection: DatabaseConnection): SystemModel = {
    new SystemModel(connection)
  }
}

class SystemModel(override protected[this] val connection: DatabaseConnection) extends DatabaseQuery {
  def deinstall(): Future[Unit] = for {
    t <- connection.begin()
    (t, _) <- t.query("DROP SCHEMA public CASCADE")
    (t, _) <- t.query("CREATE SCHEMA public")
    _ <- t.commit()
  } yield ()

  def setup(): Future[Unit] = for {
    t <- connection.begin()
    (t, _) <- t.query(s"""
                         |CREATE TABLE system_table (
                         |  table_id BIGSERIAL,
                         |  user_table_name VARCHAR(255) NOT NULL,
                         |  PRIMARY KEY(table_id)
                         |)""".stripMargin)
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
                         |)""".stripMargin)
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
                         |)""".stripMargin)

    (t, _) <- t.query(s"""
                         |CREATE TABLE folder(
                         |  id BIGSERIAL NOT NULL,
                         |  name VARCHAR(255) NOT NULL,
                         |  description VARCHAR(255) NOT NULL,
                         |  idparent BIGINT NULL,
                         |  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                         |  updated_at TIMESTAMP WITHOUT TIME ZONE,
                         |
                         |  PRIMARY KEY(id),
                         |  FOREIGN KEY(idparent)
                         |  REFERENCES folder(id) MATCH SIMPLE
                         |  ON UPDATE NO ACTION ON DELETE NO ACTION
                         |)""".stripMargin)

    (t, _) <- t.query(s"""
                         |CREATE TABLE file(
                         |  uuid UUID NOT NULL,
                         |  name VARCHAR(255) NOT NULL,
                         |  description VARCHAR(255) NULL,
                         |  mime_type VARCHAR(255) NOT NULL,
                         |  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                         |  updated_at TIMESTAMP WITHOUT TIME ZONE,
                         |  idfolder BIGINT NULL,
                         |  tmp BOOLEAN NOT NULL DEFAULT TRUE,
                         |  filename VARCHAR(255) NOT NULL,
                         |
                         |  PRIMARY KEY(uuid),
                         |  FOREIGN KEY(idfolder)
                         |  REFERENCES folder(id) MATCH SIMPLE
                         |  ON UPDATE NO ACTION ON DELETE NO ACTION
                         |)""".stripMargin)

    (t, _) <- t.query(s"""
                         |ALTER TABLE system_columns
                         |  ADD FOREIGN KEY(link_id)
                         |  REFERENCES system_link_table(link_id)
                         |  ON DELETE CASCADE""".stripMargin)
    _ <- t.commit()
  } yield ()
}