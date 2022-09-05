/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import sbt.Keys.scalaVersion

object Dependency {

  object Version {
    //CORE dependencies
    val `lz4-java` = "1.8.0"
    val `snappy-java` = "1.1.8.4"
    val `scala-java8-compat` = "1.0.2"
    val `scala-collection-compat` = "2.8.1"

    //TEST dependencies - Libraries used for tests
    val `logback-classic` = "1.2.11"
    val scalaLogging = "3.9.5"
    val scalamock = "5.2.0"
    val scalatest = "3.2.12"
    val `junit-jupiter-api` = "5.9.0"
    val `scala-parallel-collections` = "1.0.4"

    //INTEROP - Supported external libraries for serialisation & effects interop
    val boopickle = "1.4.0"
    val monix = "3.4.1"
    val zio = "2.0.1"
    val catsEffect = "3.3.14"

    //SCALA VERSIONS
    val `scala-2.12` = "2.12.16"
    val `scala-2.13` = "2.13.8"
  }

  /** Test dependencies */
  val scalatest = "org.scalatest" %% "scalatest" % Version.scalatest
  val scalamock = "org.scalamock" %% "scalamock" % Version.scalamock
  val `junit-jupiter-api` = "org.junit.jupiter" % "junit-jupiter-api" % Version.`junit-jupiter-api`
  val `lombok` = "org.projectlombok" % "lombok" % "1.18.24"
  val `scala-parallel-collections` = "org.scala-lang.modules" %% "scala-parallel-collections" % Version.`scala-parallel-collections`

  /** Compression */
  val `lz4-java` = "org.lz4" % "lz4-java" % Version.`lz4-java`
  val `snappy-java` = "org.xerial.snappy" % "snappy-java" % Version.`snappy-java`

  /** Log dependencies */
  val `logback-classic` = "ch.qos.logback" % "logback-classic" % Version.`logback-classic`
  val `scala-logging` = "com.typesafe.scala-logging" %% "scala-logging" % Version.scalaLogging

  /** Scala dependencies */
  val `scala-collection-compat` = "org.scala-lang.modules" %% "scala-collection-compat" % Version.`scala-collection-compat`
  val `scala-java8-compat` = "org.scala-lang.modules" %% "scala-java8-compat" % Version.`scala-java8-compat`
  def `scala-reflect`(scalaVersion: String) = "org.scala-lang" % "scala-reflect" % scalaVersion

  /** Interop dependencies */
  val boopickle = "io.suzaku" %% "boopickle" % Version.boopickle
  val monix = "io.monix" %% "monix" % Version.monix
  val zio = "dev.zio" %% "zio" % Version.zio
  val `cats-effect` = "org.typelevel" %% "cats-effect" % Version.catsEffect
}
