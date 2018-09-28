import sbt.Keys.{libraryDependencies, publishMavenStyle}
import sbt.url
import sbtcrossproject.CrossPlugin.autoImport.{CrossType, crossProject}
import xerial.sbt.Sonatype._

val scala211 = "2.11.12"
val scala212 = "2.12.6"

parallelExecution in ThisBuild := false

lazy val commonSettings = Seq(
  organization := "io.swaydb",
  version := "0.4",
  scalaVersion := scala212
)

val publishSettings = Seq[Setting[_]](
  crossScalaVersions := Seq(scala211, scala212),
  sonatypeProfileName := "io.swaydb",
  publishMavenStyle := true,
  licenses := Seq("AGPL3" -> url("https://www.gnu.org/licenses/agpl-3.0.en.html")),
  publish := {},
  publishLocal := {},
  sonatypeProjectHosting := Some(GitHubHosting("simerplaha", "SwayDB", "simer.j@gmail.com")),
  developers := List(
    Developer(id = "simerplaha", name = "Simer Plaha", email = "simer.j@gmail.com", url = url("http://swaydb.io"))
  ),
  publishTo := sonatypePublishTo.value
)

val testDependencies =
  Seq(
    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test,
    "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
  )

val commonDependencies =
  Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0"
  ) ++ testDependencies

lazy val SwayDB =
  (project in file("."))
    .settings(commonSettings)
    .settings(publishSettings)
    .dependsOn(embedded)
    .aggregate(embedded, core, compression, apiJVM, data, ordering, configs, serializers)

lazy val core =
  project
    .in(file("core"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies :+ "com.github.alexandrnikitin" %% "bloom-filter" % "0.10.1"
    ).dependsOn(data, macros, compression, configs % Test, ordering % Test, serializers % Test)

lazy val api =
  crossProject(JSPlatform, JVMPlatform)
    .crossType(CrossType.Pure)
    .settings(publishSettings)
    .settings(commonSettings)

lazy val apiJVM = api.jvm.dependsOn(data, serializers)
lazy val apiJS = api.js.dependsOn(data, serializers)


lazy val data =
  project
    .in(file("data"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= testDependencies
    ).dependsOn(macros)

lazy val embedded =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= commonDependencies
    ).dependsOn(apiJVM, core, configs, ordering)
    .dependsOn(core % "compile->compile;test->test")

lazy val ordering =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= commonDependencies
    ).dependsOn(data)

lazy val configs =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .dependsOn(data)

lazy val serializers =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .dependsOn(data)

lazy val compression =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies
          :+ "org.lz4" % "lz4-java" % "1.4.1"
          :+ "org.xerial.snappy" % "snappy-java" % "1.1.7"
    )
    .dependsOn(data, serializers % "compile->compile;test->test")

lazy val macros =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.12.6"
    )
