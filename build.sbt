import sbt.Keys.{libraryDependencies, publishMavenStyle}
import sbt.url
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import xerial.sbt.Sonatype._
import Dependency.Version

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
  ThisBuild / scalaVersion := Version.`scala-2.13`,
  ThisBuild / parallelExecution := false,
  scalacOptions ++= scalaOptions,
  Compile / unmanagedSourceDirectories += {
    val sourceDir = (Compile / sourceDirectory).value
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, n)) if n >= 13 =>
        sourceDir / "scala-2.13"

      case _ =>
        sourceDir / "scala-2.12"
    }
  }
)

lazy val supportedScalaVersions = List(Version.`scala-2.12`, Version.`scala-2.13`)

val publishSettings = Seq[Setting[_]](
  crossScalaVersions := supportedScalaVersions,
  sonatypeProfileName := "io.swaydb",
  publishMavenStyle := true,
  licenses := Seq("APL2" -> url("https://github.com/simerplaha/SwayDB/blob/master/LICENSE")),
  publish := {},
  publishLocal := {},
  sonatypeProjectHosting := Some(GitHubHosting("simerplaha", "SwayDB", "simer.j@gmail.com")),
  developers := List(
    Developer(id = "simerplaha", name = "Simer JS Plaha", email = "simer.j@gmail.com", url = url("https://swaydb.io"))
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

def scalaParallelCollections(scalaVersion: String, scope: sbt.librarymanagement.Configuration): Option[ModuleID] =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, major)) if major >= 13 =>
      Some(Dependency.`scala-parallel-collections` % scope)

    case _ =>
      None
  }

def testDependencies(scalaVersion: String) =
  Seq(
    Dependency.scalatest % Test,
    Dependency.`scalamock` % Test,
    Dependency.`logback-classic` % Test,
    Dependency.`boopickle` % Test
  ) ++ scalaParallelCollections(scalaVersion, Test)

val commonJavaDependencies =
  Seq(
    Dependency.`junit-jupiter-api` % Test,
    Dependency.`scalatest` % Test,
    Dependency.`scala-collection-compat` % Test,
    Dependency.`scala-java8-compat` % Test,
    Dependency.`lombok` % Test
  )

def commonDependencies(scalaVersion: String) =
  Seq(
    Dependency.`scala-logging`,
    Dependency.`scala-collection-compat`,
    Dependency.`scala-java8-compat`
  ) ++ testDependencies(scalaVersion)

lazy val SwayDB =
  (project in file("."))
    .settings(commonSettings)
    .settings(publishSettings)
    .dependsOn(`swaydb-scala`)
    .aggregate(
      actor,
      utils,
      effect,
      core,
      `core-cache`,
      `core-config`,
      `core-compression`,
      stream,
      `swaydb-scala`,
      //      `swaydb-java`,
      configs,
      serializer,
      `interop-boopickle`,
      `interop-monix`,
      `interop-zio`,
      `interop-cats-effect`
    )

lazy val testkit =
  project
    .in(file("swaydb/testkit"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies(scalaVersion.value) ++
          scalaParallelCollections(scalaVersion.value, Compile) :+
          Dependency.`scalatest`
    )

lazy val utils =
  project
    .in(file("swaydb/utils"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(testkit % Test)

lazy val effect =
  project
    .in(file("swaydb/effect"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(slice, utils, testkit % Test)

lazy val core =
  project
    .in(file("swaydb/core"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      actor,
      slice,
      effect,
      utils,
      `core-config`,
      `core-queue`,
      `core-cache`,
      `core-compression`,
      `core-skiplist`,
      testkit % Test,
      macros % "test->test;compile-internal",
      configs % Test,
      serializer % Test
    )

lazy val `core-config` =
  project
    .in(file("swaydb/core-config"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      macros % "compile-internal",
      effect,
      actor,
      utils,
      slice,
      testkit % Test
    )

lazy val `core-skiplist` =
  project
    .in(file("swaydb/core-skiplist"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      `core-series`,
      effect,
      slice,
      testkit % Test,
      serializer % Test
    )

lazy val `core-series` =
  project
    .in(file("swaydb/core-series"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      effect % Test,
      testkit % Test,
      serializer % Test
    )

lazy val `core-queue` =
  project
    .in(file("swaydb/core-queue"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(utils)

lazy val slice =
  project
    .in(file("swaydb/slice"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      utils,
      testkit % Test
    )

lazy val actor =
  project
    .in(file("swaydb/actor"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(effect, `core-cache`)

lazy val stream =
  project
    .in(file("swaydb/stream"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(effect, utils, testkit % Test)

lazy val `core-cache` =
  project
    .in(file("swaydb/core-cache"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      effect,
      utils,
      testkit % Test
    )

lazy val `swaydb-scala` =
  project
    .in(file("swaydb/swaydb-scala"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      serializer,
      configs,
      stream,
      core % "test->test;compile->compile",
      `interop-boopickle` % Test
    )

//lazy val `swaydb-java` =
//  project
//    .settings(commonSettings)
//    .settings(publishSettings)
//    .settings(libraryDependencies ++= commonJavaDependencies)
//    .dependsOn(swaydb)

lazy val configs =
  project
    .in(file("swaydb/configs"))
    .settings(commonSettings)
    .settings(publishSettings)
    .dependsOn(`core-config`)

lazy val serializer =
  project
    .in(file("swaydb/serializer"))
    .settings(commonSettings)
    .settings(publishSettings)
    .dependsOn(slice)

lazy val `core-stress` =
  project
    .in(file("swaydb/core-stress"))
    .settings(commonSettings)
    .settings(libraryDependencies ++= testDependencies(scalaVersion.value))
    .dependsOn(core)

lazy val `core-performance` =
  project
    .in(file("swaydb/core-performance"))
    .settings(commonSettings)
    .settings(libraryDependencies ++= testDependencies(scalaVersion.value))
    .dependsOn(core)

lazy val `core-compression` =
  project
    .in(file("swaydb/core-compression"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies(scalaVersion.value)
          :+ Dependency.`lz4-java`
          :+ Dependency.`snappy-java`
    )
    .dependsOn(`core-config`, serializer % Test)

lazy val macros =
  project
    .in(file("swaydb/macros"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .settings(libraryDependencies += Dependency.`scala-reflect`(scalaVersion.value))

lazy val stress =
  project
    .in(file("swaydb/stress"))
    .settings(commonSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(core, configs)
    .dependsOn(`swaydb-scala`, core % Test)

lazy val `core-tools` =
  project
    .in(file("swaydb/core-tools"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(core)

/**
 * Support modules - Effect
 */
lazy val `interop-monix` =
  project
    .in(file("swaydb/interop-monix"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += Dependency.monix)
    .dependsOn(effect)

lazy val `interop-zio` =
  project
    .in(file("swaydb/interop-zio"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += Dependency.zio)
    .dependsOn(effect)

lazy val `interop-cats-effect` =
  project
    .in(file("swaydb/interop-cats-effect"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += Dependency.`cats-effect`)
    .dependsOn(effect)

/**
 * Support modules - Serialisers.
 */
lazy val `interop-boopickle` =
  project
    .in(file("swaydb/interop-boopickle"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += Dependency.boopickle)
    .dependsOn(serializer, slice)

lazy val tools =
  project
    .in(file("swaydb/tools"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(`swaydb-scala`, `core-tools`)
