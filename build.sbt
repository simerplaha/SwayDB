import sbt.Keys.{libraryDependencies, publishMavenStyle}
import sbt.url
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import xerial.sbt.Sonatype._

//CORE dependencies
val lz4Version = "1.7.1"
val snappyVersion = "1.1.8.1"
val scalaJava8CompatVersion = "0.9.1"
val scalaCollectionsCompat = "2.3.2"

//TEST dependencies - Libraries used for tests
val logbackClassicVersion = "1.2.3"
val scalaLoggingVersion = "3.9.2"
val scalaMockVersion = "5.1.0"
val scalaTestVersion = "3.2.2"
val junitJupiterVersion = "5.7.0"
val scalaParallelCollectionsVersion = "1.0.0"

//INTEROP - Supported external libraries for serialisation & effects interop
val boopickleVersion = "1.3.3"
val monixVersion = "3.3.0"
val zioVersion = "1.0.3"
val catsEffectVersion = "2.2.0"

//SCALA VERSIONS
val scala212 = "2.12.13"
val scala213 = "2.13.4"

val inlining =
  Seq(
    //    "-opt:l:inline",
    //    "-opt-warnings",
    //    "-opt-inline-from:swaydb.**",
    //    "-Yopt-log-inline",
    //    "_"
  )

val scalaOptions =
  Seq(
    "-language:postfixOps",
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-unchecked",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-Xlint"
  ) ++ inlining

val commonSettings = Seq(
  organization := "io.swaydb",
  scalaVersion := scalaVersion.value,
  scalaVersion in ThisBuild := scala213,
  parallelExecution in ThisBuild := false,
  scalacOptions ++= scalaOptions,
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

lazy val supportedScalaVersions = List(scala212, scala213)

val publishSettings = Seq[Setting[_]](
  crossScalaVersions := supportedScalaVersions,
  sonatypeProfileName := "io.swaydb",
  publishMavenStyle := true,
  licenses := Seq("LAGPL3" -> url("https://github.com/simerplaha/SwayDB/blob/master/LICENSE.md")),
  publish := {},
  publishLocal := {},
  sonatypeProjectHosting := Some(GitHubHosting("simerplaha", "SwayDB", "simer.j@gmail.com")),
  developers := List(
    Developer(id = "simerplaha", name = "Simer JS Plaha", email = "simer.j@gmail.com", url = url("http://swaydb.io"))
  ),
  scalacOptions ++= scalaOptions,
  publishTo := sonatypePublishTo.value,
  releaseCrossBuild := true,
  releaseVersionBump := sbtrelease.Version.Bump.Next,
  publishConfiguration := publishConfiguration.value.withOverwrite(true),
  releaseProcess :=
    Seq[ReleaseStep](
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
    "org.junit.jupiter" % "junit-jupiter-api" % junitJupiterVersion % Test,
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
    "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionsCompat % Test,
    "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatVersion % Test,
    "org.projectlombok" % "lombok" % "1.18.16" % Test
  )

def commonDependencies(scalaVersion: String) =
  Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
    "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionsCompat,
    "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatVersion
  ) ++ testDependencies(scalaVersion)

lazy val SwayDB =
  (project in file("."))
    .settings(name := "SwayDB-source")
    .settings(commonSettings)
    .settings(publishSettings)
    .dependsOn(swaydb)
    .aggregate(
      swaydb,
      core,
      compression,
      data,
      actor,
      stream,
      configs,
      serializers,
      `data-java`,
      `swaydb-java`,
      `swaydb-boopickle`,
      `swaydb-monix`,
      `swaydb-zio`,
      `swaydb-cats-effect`
    )

lazy val core =
  project
    .in(file("core"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(data, actor, data % "test->test", macros % "test->test;compile-internal", compression, configs % "test->test", serializers % "test->test")

lazy val data =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(macros % "compile-internal")

lazy val actor =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(data)

lazy val stream =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(data)

lazy val `data-java` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonJavaDependencies)
    .dependsOn(data, actor, stream)

lazy val swaydb =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(core % "test->test;compile->compile", serializers, `swaydb-boopickle` % "test->test", configs, stream)

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
    .settings(libraryDependencies ++= testDependencies(scalaVersion.value))
    .dependsOn(core)

lazy val `core-performance` =
  project
    .settings(commonSettings)
    .settings(libraryDependencies ++= testDependencies(scalaVersion.value))
    .dependsOn(core)

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
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .settings(libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value)

lazy val `swaydb-stress` =
  project
    .settings(commonSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(core, configs)
    .dependsOn(swaydb, core % "test->test")

lazy val `swaydb-java` =
  project
    .settings(name := "java")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonJavaDependencies)
    .dependsOn(swaydb, `data-java`)

/**
 * Support modules - Effect
 */
lazy val `swaydb-monix` =
  project
    .settings(name := "monix")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += "io.monix" %% "monix" % monixVersion)
    .dependsOn(data)

lazy val `swaydb-zio` =
  project
    .settings(name := "zio")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += "dev.zio" %% "zio" % zioVersion)
    .dependsOn(data)

lazy val `swaydb-cats-effect` =
  project
    .settings(name := "cats-effect")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += "org.typelevel" %% "cats-effect" % catsEffectVersion)
    .dependsOn(data)

/**
 * Support modules - Serialisers.
 */
lazy val `swaydb-boopickle` =
  project
    .settings(name := "boopickle")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += "io.suzaku" %% "boopickle" % boopickleVersion)
    .dependsOn(serializers)
