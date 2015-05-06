import _root_.io.vertx.scala.build.{VertxModule, VertxProject}

import sbt._
import sbt.Keys._

object TableauxBuild extends VertxProject {
  val module = VertxModule("com.campudus", "tableaux", "0.1.0", "tables into sheets", Some("2.11.2"), Seq("2.10.4", "2.11.2"))

  lazy val copyResourcesFromCore = TaskKey[Unit]("copy-schemas-from-core", "Copy the resources from cms-core into the resources directory")

  override lazy val customSettings = Seq(
    libraryDependencies ++= Seq(
      "io.vertx" %% "mod-mysql-postgresql" % "0.3.1" % "provided",
      "io.vertx" % "testtools" % "2.0.3-final" % "test",
      "org.hamcrest" % "hamcrest-library" % "1.3" % "test",
      "com.novocode" % "junit-interface" % "0.10" % "test"
    ),
    copyResourcesFromCoreTask,
    copyMod <<= copyMod dependsOn copyResourcesFromCore,
    runMod <<= runMod dependsOn copyMod
  )

  lazy val copyResourcesFromCoreTask = copyResourcesFromCore := {
    implicit val log = streams.value.log
    copyDirectory((resourceDirectory in Compile).value / "", target.value / "mods" / "resources")
  }
}