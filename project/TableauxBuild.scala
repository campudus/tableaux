import _root_.io.vertx.scala.build.{VertxModule, VertxProject}

import sbt._
import sbt.Keys._

object TableauxBuild extends VertxProject {
  val module = VertxModule("com.campudus", "tableaux", "0.1.0", "tables into sheets", Some("2.11.7"), Seq("2.11.7"))

  override lazy val customSettings = Seq(
    libraryDependencies ++= Seq(
      "joda-time" % "joda-time" % "2.8",
      "io.vertx" %% "mod-mysql-postgresql" % "0.3.1" % "provided"
    )
  )
}