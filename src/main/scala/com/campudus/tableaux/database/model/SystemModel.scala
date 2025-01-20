package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.database.DbTransaction
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
    * Runs all needed setup functions.
    */
  def install(version: Option[Int] = None): Future[Unit] = {
    for {
      t <- connection.begin()

      // retrieve but ignore version
      (t, installedVersions) <- retrieveCurrentVersion(t)

      t <- version
        .map(i => setupFunctions.take(i))
        .getOrElse(setupFunctions)
        .drop(installedVersions)
        .foldLeft(Future(t)) {
          case (t, setup) =>
            t.flatMap(setup)
        }

      _ = logger.info("Setup schema finished")

      _ <- t.commit()
    } yield ()
  }

  /**
    * Runs all setup functions.
    */
  def installShortCutFunction(): Future[Unit] = {
    for {
      t <- connection.begin()

      // retrieve but ignore version
      (t, _) <- retrieveCurrentVersion(t)

      t <- setupShortCutFunction
        .foldLeft(Future(t)) {
          case (t, setup) =>
            t.flatMap(setup)
        }

      _ = logger.info("Setup shortcut schema finished")

      _ <- t.commit()
    } yield ()
  }

  /**
    * Runs only necessary setup functions based on current version.
    *
    * @see
    *   SystemModel#retrieveCurrentVersion
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
  def uninstall(): Future[Unit] = {
    for {
      t <- connection.begin()
      (t, _) <- t.query("DROP SCHEMA public CASCADE")
      (t, _) <- t.query("CREATE SCHEMA public")
      _ <- t.commit()
    } yield ()
  }

  /**
    * Current specification version is defined by the count of setup functions.
    *
    * @return
    *   Current specification version
    */
  def retrieveSpecificationVersion(): Int = setupFunctions.size

  /**
    * Creates system_version tables if it doesn't exist. Each entry in the system_version tables defines one incremental
    * update of the system structure (all system tables need for tableaux to work).
    *
    * @see
    *   SystemModel#retrieveCurrentVersion
    * @return
    *   current version
    */
  def retrieveCurrentVersion(): Future[Int] = {
    for {
      t <- connection.begin()
      (t, version) <- retrieveCurrentVersion(t)
      _ <- t.commit()
    } yield version
  }

  private def retrieveCurrentVersion(t: DbTransaction): Future[(DbTransaction, Int)] = {
    for {
      (t, _) <- t.query(s"""
                           |CREATE TABLE IF NOT EXISTS system_version(
                           |version INT NOT NULL,
                           |updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                           |PRIMARY KEY(version)
                           |)
       """.stripMargin)

      (t, version) <- {
        t.query(
          "SELECT max_version FROM (SELECT MAX(version) AS max_version FROM system_version) sub WHERE max_version IS NOT NULL"
        ) map {
          case (t, result) =>
            val version = Try(selectNotNull(result).head.getInteger(0).toInt).getOrElse(-1)
            (t, version)
        }
      }
    } yield (t, version)
  }

  private val setupFunctions: Seq[DbTransaction => Future[DbTransaction]] = Seq(
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
    setupVersion(readSchemaFile("schema_v16"), 16),
    setupVersion(readSchemaFile("schema_v17"), 17),
    setupVersion(readSchemaFile("schema_v18"), 18),
    setupVersion(readSchemaFile("schema_v19"), 19),
    setupVersion(readSchemaFile("schema_v20"), 20),
    setupVersion(readSchemaFile("schema_v21"), 21),
    setupVersion(readSchemaFile("schema_v22"), 22),
    setupVersion(readSchemaFile("schema_v23"), 23),
    setupVersion(readSchemaFile("schema_v24"), 24),
    setupVersion(readSchemaFile("schema_v25"), 25),
    setupVersion(readSchemaFile("schema_v26"), 26),
    setupVersion(readSchemaFile("schema_v27"), 27),
    setupVersion(readSchemaFile("schema_v28"), 28),
    setupVersion(readSchemaFile("schema_v29"), 29),
    setupVersion(readSchemaFile("schema_v30"), 30),
    setupVersion(readSchemaFile("schema_v31"), 31),
    setupVersion(readSchemaFile("schema_v32"), 32),
    setupVersion(readSchemaFile("schema_v33"), 33),
    setupVersion(readSchemaFile("schema_v34"), 34),
    setupVersion(readSchemaFile("schema_v35"), 35),
    setupVersion(readSchemaFile("schema_v36"), 36),
    setupVersion(readSchemaFile("schema_v37"), 37),
    setupVersion(readSchemaFile("schema_v38"), 38)
  )

  private val setupShortCutFunction: Seq[DbTransaction => Future[DbTransaction]] = Seq(
    setupVersion(readSchemaFile("merged_schema_until_v35"), 35)
  )

  private def readSchemaFile(name: String): String = {
    Source.fromInputStream(getClass.getResourceAsStream(s"/schema/$name.sql"), "UTF-8").mkString
  }

  private def setupVersion(stmt: String, versionId: Int)(t: DbTransaction): Future[DbTransaction] = {
    logger.debug(s"Setup schema version $versionId")

    for {
      t <- t
        .query(stmt)
        .map({
          case (t, _) => t
        })
      t <- saveVersion(t, versionId)
    } yield t
  }

  private def saveVersion(t: DbTransaction, versionId: Int): Future[DbTransaction] = {
    t.query(
      s"""
         |INSERT INTO system_version (version)
         |SELECT ? WHERE NOT EXISTS (SELECT version FROM system_version WHERE version = ?)
       """.stripMargin,
      Json.arr(versionId, versionId)
    ) map {
      case (t, _) => t
    }
  }

  def retrieveSetting(key: String): Future[Option[String]] = {
    connection
      .query("SELECT value FROM system_settings WHERE key = ?", Json.arr(key))
      .map(json => resultObjectToJsonArray(json).headOption.flatMap(row => Option(row.getString(0))))
  }

  def updateSetting(key: String, value: String): Future[Unit] = {
    for {
      // first make sure that the key exists...
      _ <- connection
        .query(
          """
            |INSERT INTO system_settings (key)
            |SELECT ? WHERE NOT EXISTS (SELECT key FROM system_settings WHERE key = ?)
            |""".stripMargin,
          Json.arr(key, key)
        )

      // ... then update the value
      _ <- connection
        .query("UPDATE system_settings SET value = ? WHERE key = ?", Json.arr(value, key))
        .map(json => updateNotNull(json))
    } yield ()
  }
}
