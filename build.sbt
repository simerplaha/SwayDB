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
val junitJupiterVersion = "5.8.2"
val scalaParallelCollectionsVersion = "1.0.4"

//INTEROP - Supported external libraries for serialisation & effects interop
val boopickleVersion = "1.4.0"
val monixVersion = "3.4.0"
val zioVersion = "1.0.12"
val catsEffectVersion = "3.3.0"

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
    .dependsOn(`swaydb-scala-api`)
    .aggregate(
      actor,
      utils,
      effect,
      core,
      `core-cache`,
      `core-config`,
      `core-compression`,
      stream,
      `swaydb-scala-api`,
      //      `swaydb-java-api`,
      configs,
      serializers,
      `interop-boopickle`,
      `interop-monix`,
      `interop-zio`,
      `interop-cats-effect`
    )

lazy val testkit =
  project
    .in(file("libraries/testkit"))
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
    .in(file("libraries/utils"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(testkit % Test)

lazy val effect =
  project
    .in(file("libraries/effect"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(slice, utils, testkit % Test)

lazy val core =
  project
    .in(file("core/core"))
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
      serializers % Test
    )

lazy val `core-config` =
  project
    .in(file("core/config"))
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
    .in(file("core/skiplist"))
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
    .in(file("core/series"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      effect % Test,
      testkit % Test,
      serializers % Test
    )

lazy val `core-queue` =
  project
    .in(file("core/queue"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(utils)

lazy val slice =
  project
    .in(file("libraries/slice"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(utils, testkit % Test)

lazy val actor =
  project
    .in(file("libraries/actor"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(effect, `core-cache`)

lazy val stream =
  project
    .in(file("libraries/stream"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(effect, utils, testkit % Test)

lazy val `core-cache` =
  project
    .in(file("core/cache"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      effect,
      utils,
      testkit % Test
    )

lazy val `swaydb-scala-api` =
  project
    .in(file("swaydb/scala-api"))
    .settings(name := "scala-api")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(
      serializers,
      configs,
      stream,
      core % "test->test;compile->compile",
      `interop-boopickle` % Test
    )

//lazy val `swaydb-java-api` =
//  project
//    .in(file("swaydb/java-api"))
//    .settings(name := "java-api")
//    .settings(commonSettings)
//    .settings(publishSettings)
//    .settings(libraryDependencies ++= commonJavaDependencies)
//    .dependsOn(`swaydb-scala-api`)

lazy val configs =
  project
    .in(file("swaydb/configs"))
    .settings(commonSettings)
    .settings(publishSettings)
    .dependsOn(`core-config`)

lazy val serializers =
  project
    .in(file("swaydb/serializers"))
    .settings(commonSettings)
    .settings(publishSettings)
    .dependsOn(slice)

lazy val `core-stress` =
  project
    .in(file("core/stress"))
    .settings(commonSettings)
    .settings(libraryDependencies ++= testDependencies(scalaVersion.value))
    .dependsOn(core)

lazy val `core-performance` =
  project
    .in(file("core/performance"))
    .settings(commonSettings)
    .settings(libraryDependencies ++= testDependencies(scalaVersion.value))
    .dependsOn(core)

lazy val `core-compression` =
  project
    .in(file("core/compression"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        commonDependencies(scalaVersion.value)
          :+ "org.lz4" % "lz4-java" % lz4Version
          :+ "org.xerial.snappy" % "snappy-java" % snappyVersion
    )
    .dependsOn(`core-config`, serializers % Test)

lazy val macros =
  project
    .in(file("libraries/macros"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .settings(libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value)

lazy val stress =
  project
    .in(file("swaydb/stress"))
    .settings(commonSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(core, configs)
    .dependsOn(`swaydb-scala-api`, core % Test)


lazy val `core-tools` =
  project
    .in(file("core/tools"))
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(core)

/**
 * Support modules - Effect
 */

lazy val `interop-monix` =
  project
    .in(file("swaydb/x-interop-monix"))
    .settings(name := "monix")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += "io.monix" %% "monix" % monixVersion)
    .dependsOn(effect)

lazy val `interop-zio` =
  project
    .in(file("swaydb/x-interop-zio"))
    .settings(name := "zio")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += "dev.zio" %% "zio" % zioVersion)
    .dependsOn(effect)

lazy val `interop-cats-effect` =
  project
    .in(file("swaydb/x-interop-cats-effect"))
    .settings(name := "cats-effect")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += "org.typelevel" %% "cats-effect" % catsEffectVersion)
    .dependsOn(effect)

/**
 * Support modules - Serialisers.
 */
lazy val `interop-boopickle` =
  project
    .in(file("swaydb/x-interop-boopickle"))
    .settings(name := "boopickle")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies += "io.suzaku" %% "boopickle" % boopickleVersion)
    .dependsOn(serializers, slice)

lazy val tools =
  project
    .in(file("swaydb/tools"))
    .settings(name := "tools")
    .settings(commonSettings)
    .settings(publishSettings)
    .settings(libraryDependencies ++= commonDependencies(scalaVersion.value))
    .dependsOn(`swaydb-scala-api`, `core-tools`)
