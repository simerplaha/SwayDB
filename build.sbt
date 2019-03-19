import sbt.Keys.{libraryDependencies, publishMavenStyle}
import sbt.url
import xerial.sbt.Sonatype._

val scala211 = "2.11.12"
val scala212 = "2.12.8"
val lz4Version = "1.5.1"
val snappyVersion = "1.1.7"
val logbackClassicVersion = "1.2.3"
val bloomFilterVersion = "0.11.0"
val scalaLoggingVersion = "3.9.0"
val scalaMockVersion = "4.1.0"
val scalaTestVersion = "3.0.5"

parallelExecution in ThisBuild := false

lazy val commonSettings = Seq(
  organization := "io.swaydb",
  version := "0.7.1",
  scalaVersion := scalaVersion.value
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
    "org.scalamock" %% "scalamock" % scalaMockVersion % Test,
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
    .aggregate(extension, access, core, compression, data, configs, serializers)

lazy val core =
  project
    .in(file("core"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies
          :+ "com.github.alexandrnikitin" %% "bloom-filter" % bloomFilterVersion
    ).dependsOn(data, macros % "compile-internal", compression, configs % Test, serializers % Test)

lazy val data =
  project
    .in(file("data"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= testDependencies
    ).dependsOn(macros % "compile-internal")

lazy val access =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= commonDependencies
    ).dependsOn(core, configs)
    .dependsOn(serializers, core % Test)

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
    ).dependsOn(access, core % Test)

lazy val `core-stress` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= testDependencies
    ).dependsOn(core)

lazy val `core-performance` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= testDependencies
    ).dependsOn(core)

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
    .dependsOn(data, serializers % Test)

lazy val macros =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
    )

lazy val `access-stress` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies
          :+ "com.github.simerplaha" %% "actor" % "0.3" % Test
          :+ "io.suzaku" %% "boopickle" % "1.3.0" % Test
    ).dependsOn(core, configs)
    .dependsOn(access, core % Test)

lazy val benchmark =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= commonDependencies
    ).dependsOn(core, configs)
    .dependsOn(access, core % Test)