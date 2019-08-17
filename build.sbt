import sbt.Keys.{libraryDependencies, publishMavenStyle}
import sbt.url
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import xerial.sbt.Sonatype._

val lz4Version = "1.6.0"
val snappyVersion = "1.1.7.3"
val logbackClassicVersion = "1.2.3"
val scalaLoggingVersion = "3.8.0"
val scalaMockVersion = "4.4.0"
val scalaTestVersion = "3.0.8"
val reactiveStreamsVersion = "1.0.2"
val boopickleVersion = "1.3.1"

parallelExecution in ThisBuild := false

lazy val commonSettings = Seq(
  organization := "io.swaydb",
  version := "0.9",
  scalaVersion := scalaVersion.value
)

val publishSettings = Seq[Setting[_]](
  crossScalaVersions := Seq("2.11.12", "2.12.9"),
  sonatypeProfileName := "io.swaydb",
  publishMavenStyle := true,
  licenses := Seq("AGPL3" -> url("https://www.gnu.org/licenses/agpl-3.0.en.html")),
  publish := {},
  publishLocal := {},
  sonatypeProjectHosting := Some(GitHubHosting("simerplaha", "SwayDB", "simer.j@gmail.com")),
  developers := List(
    Developer(id = "simerplaha", name = "Simer Plaha", email = "simer.j@gmail.com", url = url("http://swaydb.io"))
  ),
  publishTo := sonatypePublishTo.value,
  releaseCrossBuild := true,
  releaseVersionBump := sbtrelease.Version.Bump.Next,
  publishConfiguration := publishConfiguration.value.withOverwrite(true),
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    releaseStepCommandAndRemaining("+publishSigned"),
    setNextVersion,
    commitNextVersion,
    releaseStepCommand("sonatypeReleaseAll"),
    pushChanges
  )
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
    .dependsOn(api)
    .aggregate(api, core, compression, data, configs, serializers)

lazy val core =
  project
    .in(file("core"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= commonDependencies
    ).dependsOn(data, macros % "test->test;compile-internal", compression, configs % "test->test", serializers % "test->test")

lazy val data =
  project
    .in(file("data"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= commonDependencies
    ).dependsOn(macros % "compile-internal")

lazy val api =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies
          :+ "org.reactivestreams" % "reactive-streams" % reactiveStreamsVersion

    ).dependsOn(core % "test->test;compile->compile", serializers, configs)

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

lazy val `core-stress` =
  project
    .settings(commonSettings)
    .settings(
      libraryDependencies ++= testDependencies
    ).dependsOn(core)

lazy val `core-performance` =
  project
    .settings(commonSettings)
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
    .dependsOn(data, serializers % "test->test")

lazy val macros =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
    )

lazy val `api-stress` =
  project
    .settings(commonSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies :+ "io.suzaku" %% "boopickle" % boopickleVersion % Test
    ).dependsOn(core, configs)
    .dependsOn(api, core % "test->test")

lazy val benchmark =
  project
    .settings(commonSettings)
    .settings(
      libraryDependencies ++= commonDependencies
    ).dependsOn(core, configs)
    .dependsOn(api, core % "test->test")
