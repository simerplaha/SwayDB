import sbt.Keys.{libraryDependencies, publishMavenStyle}
import sbt.url
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import xerial.sbt.Sonatype._

val lz4Version = "1.7.1"
val snappyVersion = "1.1.7.3"
val logbackClassicVersion = "1.2.3"
val scalaLoggingVersion = "3.9.2"
val scalaMockVersion = "4.4.0"
val scalaTestVersion = "3.0.8"
val reactiveStreamsVersion = "1.0.2"
val boopickleVersion = "1.3.1"
val monixVersion = "3.1.0"
val zioVersion = "1.0.0-RC17"
val catsEffectVersion = "2.0.0"
val scalaJava8CompatVersion = "0.9.1"
val junitJupiterVersion = "5.6.0"
val scalaParallelCollectionsVersion = "0.2.0"
val scalaCollectionsCompact = "2.1.4"

val scala211 = "2.11.12"
val scala212 = "2.12.11"
val scala213 = "2.13.1"

val commonScalaOptions =
  Seq(
    "-language:postfixOps",
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-language:higherKinds",
    "-language:implicitConversions",
    //    "-Ywarn-dead-code",
    //    "-Ywarn-numeric-widen",
    //    "-Ywarn-value-discard",
    //    "-Ywarn-unused",
    //    "-Xfatal-warnings",
    "-Xlint"
  )

def publishScalaOptions(scalaVersion: String): Seq[String] = {
  val publishOptions: Seq[String] =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, major)) if major >= 13 =>
        Seq(
          "-opt:l:inline",
          "-opt-warnings",
          "-opt-inline-from:swaydb.**",
          "-Yopt-log-inline",
          "_"
        )

      case Some((2, 12)) =>
        //todo
        Seq.empty

      case Some((2, 11)) =>
        //todo
        Seq.empty
    }

  publishOptions ++ commonScalaOptions
}

val commonSettings = Seq(
  organization := "io.swaydb",
  scalaVersion := scalaVersion.value,
  scalaVersion in ThisBuild := scala213,
  parallelExecution in ThisBuild := false,
  scalacOptions ++= commonScalaOptions,
  unmanagedSourceDirectories in Compile += {
    val sourceDir = (sourceDirectory in Compile).value
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, n)) if n >= 13 =>
        sourceDir / "scala-2.13"

      case _ =>
        sourceDir / "scala-2.12"
    }
  }
)

val publishSettings = Seq[Setting[_]](
  crossScalaVersions := Seq(scala211, scala212, scala213),
  sonatypeProfileName := "io.swaydb",
  publishMavenStyle := true,
  licenses := Seq("LAGPL3" -> url("https://github.com/simerplaha/SwayDB/blob/master/LICENSE")),
  publish := {},
  publishLocal := {},
  sonatypeProjectHosting := Some(GitHubHosting("simerplaha", "SwayDB", "simer.j@gmail.com")),
  developers := List(
    Developer(id = "simerplaha", name = "Simer JS Plaha", email = "simer.j@gmail.com", url = url("http://swaydb.io"))
  ),
  scalacOptions ++= publishScalaOptions(scalaVersion.value),
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

def scalaParallelCollections(scalaVersion: String) =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, major)) if major >= 13 =>
      Some("org.scala-lang.modules" %% "scala-parallel-collections" % scalaParallelCollectionsVersion % Test)

    case _ =>
      None
  }

def testDependencies(scalaVersion: String) =
  Seq(
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
    "org.scalamock" %% "scalamock" % scalaMockVersion % Test,
    "ch.qos.logback" % "logback-classic" % logbackClassicVersion % Test,
    "io.suzaku" %% "boopickle" % boopickleVersion % Test
  ) ++ scalaParallelCollections(scalaVersion)

val commonJavaDependencies =
  Seq(
    "org.junit.jupiter" % "junit-jupiter-api" % junitJupiterVersion % Test
  )

def commonDependencies(scalaVersion: String) =
  Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
    "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionsCompact,
    "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatVersion
  ) ++ testDependencies(scalaVersion)

lazy val SwayDB =
  (project in file("."))
    .settings(name := "SwayDB.source")
    .settings(commonSettings)
    .settings(publishSettings)
    .dependsOn(swaydb)
    .aggregate(swaydb, core, compression, data, configs, serializers, `swaydb-monix`, `swaydb-zio`, `swaydb-cats-effect`, `data-java`, `swaydb-java`)

lazy val core =
  project
    .in(file("core"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= commonDependencies(scalaVersion.value)
    ).dependsOn(data, macros % "test->test;compile-internal", compression, configs % "test->test", serializers % "test->test")

lazy val data =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= commonDependencies(scalaVersion.value)
    ).dependsOn(macros % "compile-internal")

lazy val `data-java` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= commonJavaDependencies
    ).dependsOn(data)

lazy val swaydb =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies(scalaVersion.value)
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
      libraryDependencies ++= testDependencies(scalaVersion.value)
    ).dependsOn(core)

lazy val `core-performance` =
  project
    .settings(commonSettings)
    .settings(
      libraryDependencies ++= testDependencies(scalaVersion.value)
    ).dependsOn(core)

lazy val compression =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies(scalaVersion.value)
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

lazy val `swaydb-stress` =
  project
    .settings(commonSettings)
    .settings(
      libraryDependencies ++= commonDependencies(scalaVersion.value)
    ).dependsOn(core, configs)
    .dependsOn(swaydb, core % "test->test")

lazy val `swaydb-java` =
  project
    .settings(name := "java")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++= commonJavaDependencies
    )
    .dependsOn(swaydb, `data-java`)

lazy val benchmark =
  project
    .settings(commonSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies(scalaVersion.value) :+
          "ch.qos.logback" % "logback-classic" % logbackClassicVersion
    ).dependsOn(core, configs)
    .dependsOn(swaydb, core % "test->test")

/**
 * Support modules.
 */
lazy val `swaydb-monix` =
  project
    .settings(name := "monix")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies += "io.monix" %% "monix" % monixVersion
    )
    .dependsOn(data)

lazy val `swaydb-zio` =
  project
    .settings(name := "zio")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies += "dev.zio" %% "zio" % zioVersion
    )
    .dependsOn(data)

lazy val `swaydb-cats-effect` =
  project
    .settings(name := "cats-effect")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies += "org.typelevel" %% "cats-effect" % catsEffectVersion
    )
    .dependsOn(data)
