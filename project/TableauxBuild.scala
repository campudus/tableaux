import _root_.io.vertx.scala.build.{VertxModule, VertxProject}

import sbt._
import sbt.Keys._

object TableauxBuild extends VertxProject {
  val module = VertxModule("com.campudus", "tableaux", "0.1.0", "tables into sheets", Some("2.11.2"), Seq("2.10.4", "2.11.2"))

  override lazy val customSettings = Seq(
    libraryDependencies ++= Seq(
      "io.vertx" %% "mod-mysql-postgresql" % "0.3.1"
    )
  )
}
