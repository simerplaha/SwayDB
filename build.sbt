import sbt.Keys.{libraryDependencies, publishMavenStyle}
import sbt.url
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import xerial.sbt.Sonatype._

//CORE dependencies
val lz4Version = "1.8.0"
val snappyVersion = "1.1.8.4"
val scalaJava8CompatVersion = "1.0.2"
val scalaCollectionsCompat = "2.6.0"

//TEST dependencies - Libraries used for tests
val logbackClassicVersion = "1.2.7"
val scalaLoggingVersion = "3.9.4"
val scalaMockVersion = "5.1.0"
val scalaTestVersion = "3.2.10"
val junitJupiterVersion = "5.8.1"
val scalaParallelCollectionsVersion = "1.0.4"

//INTEROP - Supported external libraries for serialisation & effects interop
val boopickleVersion = "1.4.0"
val monixVersion = "3.4.0"
val zioVersion = "1.0.12"
val catsEffectVersion = "3.2.9"

//SCALA VERSIONS
val scala212 = "2.12.15"
val scala213 = "2.13.7"

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

def scalaParallelCollections(scalaVersion: String, scope: sbt.librarymanagement.Configuration): Option[ModuleID] =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, major)) if major >= 13 =>
      Some("org.scala-lang.modules" %% "scala-parallel-collections" % scalaParallelCollectionsVersion % scope)

    case _ =>
      None
  }

def testDependencies(scalaVersion: String) =
  Seq(
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
    "org.scalamock" %% "scalamock" % scalaMockVersion % Test,
    "ch.qos.logback" % "logback-classic" % logbackClassicVersion % Test,
    "io.suzaku" %% "boopickle" % boopickleVersion % Test
  ) ++ scalaParallelCollections(scalaVersion, Test)

val commonJavaDependencies =
  Seq(
    "org.junit.jupiter" % "junit-jupiter-api" % junitJupiterVersion % Test,
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
    "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionsCompat % Test,
    "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatVersion % Test,
    "org.projectlombok" % "lombok" % "1.18.22" % Test
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
      actor,
      utils,
      effect,
      core,
      `core-cache`,
      `core-interop`,
      stream,
      swaydb,
      `swaydb-java`,
      configs,
      serializers,
      `x-interop-boopickle`,
      `x-interop-monix`,
      `x-interop-zio`,
      `x-interop-cats-effect`
    )

lazy val testkit =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies(scalaVersion.value) ++
          scalaParallelCollections(scalaVersion.value, Compile) :+
          "org.scalatest" %% "scalatest" % scalaTestVersion
    )

lazy val utils =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(testkit % Test)

lazy val effect =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(utils, testkit % Test)

lazy val core =
  project
    .in(file("core"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      actor,
      slice,
      effect,
      utils,
      `core-compaction`,
      `core-level`,
      `core-log`,
      `core-segment`,
      `core-file`,
      `core-interop`,
      `core-queue`,
      `core-cache`,
      `core-compression`,
      testkit % Test,
      macros % "test->test;compile-internal",
      configs % Test,
      serializers % Test
    )

lazy val `core-interop` =
  project
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

lazy val `core-compaction` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      macros % "compile-internal",
      effect,
      `core-level`,
      actor,
      utils,
      slice,
      testkit % Test
    )

lazy val `core-level` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      macros % "compile-internal",
      slice,
      effect,
      utils,
      `core-log`,
      `core-segment`,
      testkit % Test,
    )

lazy val `core-file` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      macros % "compile-internal",
      slice,
      effect,
      utils,
      `core-interop`,
      `core-cache`,
      testkit % Test,
    )

lazy val `core-segment` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      macros % "compile-internal",
      slice,
      effect,
      utils,
      `core-compression`,
      `core-skiplist`,
      `core-file`,
      `core-interop`,
      `core-cache`,
      testkit % Test,
    )

lazy val `core-log` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      macros % "compile-internal",
      slice,
      effect,
      utils,
      `core-compression`,
      `core-skiplist`,
      `core-file`,
      `core-queue`,
      `core-segment`,
      `core-interop`,
      `core-cache`,
      testkit % Test
    )

lazy val `core-skiplist` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      `core-series`,
      effect,
      slice,
      testkit % Test,
      serializers % Test
    )

lazy val `core-series` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      testkit % Test,
      serializers % Test
    )

lazy val `core-queue` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(utils)

lazy val slice =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      effect,
      testkit % Test,
    )

lazy val actor =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(effect, `core-cache`)

lazy val stream =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(effect, utils, testkit % Test)

lazy val `core-cache` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      effect,
      utils,
      testkit % Test
    )

lazy val swaydb =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      serializers,
      configs,
      stream,
      core % "test->test;compile->compile",
      `x-interop-boopickle` % Test
    )

lazy val configs =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .dependsOn(`core-interop`)

lazy val serializers =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .dependsOn(slice)

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

lazy val `core-compression` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies(scalaVersion.value)
          :+ "org.lz4" % "lz4-java" % lz4Version
          :+ "org.xerial.snappy" % "snappy-java" % snappyVersion
    )
    .dependsOn(`core-interop`, serializers % Test)

lazy val macros =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .settings(libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value)

lazy val stress =
  project
    .settings(commonSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(core, configs)
    .dependsOn(swaydb, core % Test)

lazy val `swaydb-java` =
  project
    .settings(name := "java")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonJavaDependencies)
    .dependsOn(swaydb)

lazy val `core-tools` =
  project
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(core)

/**
 * Support modules - Effect
 */
lazy val `x-interop-monix` =
  project
    .settings(name := "monix")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += "io.monix" %% "monix" % monixVersion)
    .dependsOn(effect)

lazy val `x-interop-zio` =
  project
    .settings(name := "zio")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += "dev.zio" %% "zio" % zioVersion)
    .dependsOn(effect)

lazy val `x-interop-cats-effect` =
  project
    .settings(name := "cats-effect")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += "org.typelevel" %% "cats-effect" % catsEffectVersion)
    .dependsOn(effect)

/**
 * Support modules - Serialisers.
 */
lazy val `x-interop-boopickle` =
  project
    .settings(name := "boopickle")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += "io.suzaku" %% "boopickle" % boopickleVersion)
    .dependsOn(serializers, slice)

lazy val tools =
  project
    .settings(name := "tools")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(swaydb, `core-tools`)
