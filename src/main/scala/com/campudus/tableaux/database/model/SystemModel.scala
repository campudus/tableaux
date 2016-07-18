package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._

import scala.concurrent.Future
import scala.io.Source
import scala.util.Try

object SystemModel {
  def apply(connection: DatabaseConnection): SystemModel = {
    new SystemModel(connection)
  }
}

class SystemModel(override protected[this] val connection: DatabaseConnection) extends DatabaseQuery {

  /**
    * Runs all setup functions.
    */
  def install(version: Option[Int] = None): Future[Unit] = {
    for {
      t <- connection.begin()

      // retrieve but ignore version
      (t, _) <- retrieveCurrentVersion(t)

      t <- version
        .map(i => setupFunctions.take(i))
        .getOrElse(setupFunctions)
        .foldLeft(Future(t)) {
          case (t, setup) =>
            t.flatMap(setup)
        }

      _ <- t.commit()
    } yield ()
  }

  /**
    * Runs only necessary setup functions
    * based on current version.
    *
    * @see SystemModel#retrieveCurrentVersion
    */
  def update(): Future[Unit] = {
    for {
      t <- connection.begin()

      // retrieve current schema version
      (t, version) <- retrieveCurrentVersion(t)

      t <- setupFunctions
        .drop(version)
        .foldLeft(Future(t)) {
          case (t, setup) =>
            t.flatMap(setup)
        }

      _ <- t.commit()
    } yield ()
  }

  /**
    * Drops schema and creates a new one.
    */
  def uninstall(): Future[Unit] = for {
    t <- connection.begin()
    (t, _) <- t.query("DROP SCHEMA public CASCADE")
    (t, _) <- t.query("CREATE SCHEMA public")
    _ <- t.commit()
  } yield ()

  /**
    * Current specification version is defined by the
    * count of setup functions.
    *
    * @return Current specification version
    */
  def retrieveSpecificationVersion(): Int = setupFunctions.size

  /**
    * Creates system_version tables if it doesn't exist. Each entry in the
    * system_version tables defines one incremental update of the system
    * structure (all system tables need for tableaux to work).
    *
    * @see SystemModel#retrieveCurrentVersion
    * @return current version
    */
  def retrieveCurrentVersion(): Future[Int] = {
    for {
      t <- connection.begin()
      (t, version) <- retrieveCurrentVersion(t)
      _ <- t.commit()
    } yield version
  }

  private def retrieveCurrentVersion(t: connection.Transaction): Future[(connection.Transaction, Int)] = {
    for {
      (t, _) <- t.query(
        s"""
           |CREATE TABLE IF NOT EXISTS system_version(
           |version INT NOT NULL,
           |updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
           |PRIMARY KEY(version)
           |)
       """.stripMargin)

      (t, version) <- {
        t.query("SELECT max_version FROM (SELECT MAX(version) AS max_version FROM system_version) sub WHERE max_version IS NOT NULL") map {
          case (t, result) =>
            val version = Try(selectNotNull(result).head.getInteger(0).toInt).getOrElse(-1)
            (t, version)
        }
      }
    } yield (t, version)
  }

  private val setupFunctions: Seq[connection.Transaction => Future[connection.Transaction]] = Seq(
    setupVersion(readSchemaFile("schema_v1"), 1),
    setupVersion(readSchemaFile("schema_v2"), 2),
    setupVersion(readSchemaFile("schema_v3"), 3),
    setupVersion(readSchemaFile("schema_v4"), 4),
    setupVersion(readSchemaFile("schema_v5"), 5),
    setupVersion(readSchemaFile("schema_v6"), 6),
    setupVersion(readSchemaFile("schema_v7"), 7),
    setupVersion(readSchemaFile("schema_v8"), 8),
    setupVersion(readSchemaFile("schema_v9"), 9),
    setupVersion(readSchemaFile("schema_v10"), 10),
    setupVersion(readSchemaFile("schema_v11"), 11),
    setupVersion(readSchemaFile("schema_v12"), 12),
    setupVersion(readSchemaFile("schema_v13"), 13),
    setupVersion(readSchemaFile("schema_v14"), 14),
    setupVersion(readSchemaFile("schema_v15"), 15),
    setupVersion(readSchemaFile("schema_v16"), 16)
  )

  private def readSchemaFile(name: String): String = {
    Source.fromInputStream(getClass.getResourceAsStream(s"/schema/$name.sql"), "UTF-8").mkString
  }

  private def setupVersion(stmt: String, versionId: Int)(t: connection.Transaction): Future[connection.Transaction] = {
    logger.info(s"Setup schema version $versionId")

    for {
      t <- t.query(stmt).map(_._1)
      t <- saveVersion(t, versionId)
    } yield t
  }

  private def saveVersion(t: connection.Transaction, versionId: Int): Future[connection.Transaction] = {
    t.query(
      s"""
         |INSERT INTO system_version (version)
         |SELECT ? WHERE NOT EXISTS (SELECT version FROM system_version WHERE version = ?)
       """.stripMargin, Json.arr(versionId, versionId)) map {
      case (t, _) => t
    }
  }

  def retrieveSetting(key: String): Future[String] = {
    connection.query("SELECT value FROM system_settings WHERE key = ?", Json.arr(key))
      .map(json => selectNotNull(json).head.getString(0))
  }
}