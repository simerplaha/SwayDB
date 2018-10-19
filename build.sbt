import sbt.Keys.{libraryDependencies, publishMavenStyle}
import sbt.url
import xerial.sbt.Sonatype._

val scala211 = "2.11.12"
val scala212 = "2.12.7"
val scalaMetaVersion = "4.0.0"
val lz4Version = "1.5.0"
val snappyVersion = "1.1.7"
val logbackClassicVersion = "1.2.3"
val bloomFilterVersion = "0.11.0"
val scalaLoggingVersion = "3.8.0"
val scalaMockVersion = "3.6.0"
val scalaTestVersion = "3.0.5"

parallelExecution in ThisBuild := false

lazy val commonSettings = Seq(
  organization := "io.swaydb",
  version := "0.7",
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
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
    "org.scalamock" %% "scalamock-scalatest-support" % scalaMockVersion % Test,
    "ch.qos.logback" % "logback-classic" % logbackClassicVersion % Test
  )

val commonDependencies =
  Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion
  ) ++ testDependencies

lazy val SwayDB =
  (project in file("."))
    .settings(commonSettings)
    .settings(publishSettings)
    .dependsOn(extension)
    .aggregate(extension, embedded, core, macros, compression, data, ordering, configs, serializers)

lazy val core =
  project
    .in(file("core"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies :+ "com.github.alexandrnikitin" %% "bloom-filter" % bloomFilterVersion
    ).dependsOn(data, macros, compression, configs % Test, ordering % Test, serializers % Test)

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
    ).dependsOn(core, configs, ordering)
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


lazy val extension =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= testDependencies
    ).dependsOn(embedded)

lazy val compression =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies
          :+ "org.lz4" % "lz4-java" % lz4Version
          :+ "org.xerial.snappy" % "snappy-java" % snappyVersion
    )
    .dependsOn(data, serializers % "compile->compile;test->test")

lazy val macros =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies += "org.scala-lang" % "scala-reflect" % scala212,
      libraryDependencies += "org.scala-lang" % "scala-compiler" % scala212,
      libraryDependencies += "org.scalameta" %% "scalameta" % scalaMetaVersion
    )
