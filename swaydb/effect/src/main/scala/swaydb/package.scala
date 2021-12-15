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

import com.typesafe.scalalogging.LazyLogging
import swaydb.effect.Dir

import java.nio.file.{Path, Paths}

package object swaydb extends LazyLogging {

  //transparent type
  type Glass[+A] = A

  implicit def stringToPath(path: String): Path =
    Paths.get(path)

  implicit def pathStringToDir(path: String): Dir =
    Dir(Paths.get(path), 1)

  implicit def pathToDir(path: Path): Dir =
    Dir(path, 1)

  implicit def pathsToDirs(paths: Path*): Seq[Dir] =
    pathSeqToDirs(paths)

  implicit def pathSeqToDirs(paths: Seq[Path]): Seq[Dir] =
    paths.map(Dir(_, 1))

  implicit def tupleToDir(dir: (Path, Int)): Dir =
    Dir(dir._1, dir._2)

  implicit def tupleToDirs(dir: (Path, Int)): Seq[Dir] =
    Seq(Dir(dir._1, dir._2))

  implicit def tupleStringToDirs(dir: (String, Int)): Seq[Dir] =
    Seq(Dir(dir._1, dir._2))

  implicit def tuplesToDirs(dir: (Path, Int)*): Seq[Dir] =
    tupleSeqToDirs(dir)

  implicit def dirToDirs(dir: Dir*): Seq[Dir] =
    Seq(dir: _*)

  implicit def tupleSeqToDirs(dir: Seq[(Path, Int)]): Seq[Dir] =
    dir.map {
      case (path, dist) =>
        Dir(path, dist)
    }

  implicit def tupleStringSeqToDirs(dir: Seq[(String, Int)]): Seq[Dir] =
    dir.map {
      case (path, dist) =>
        Dir(path, dist)
    }

  implicit def pathToDirs(dir: Path): Seq[Dir] =
    Seq(Dir(dir, 1))

  implicit class StorageIntImplicits(measure: Int) {

    @inline final def bytes: Int = measure

    @inline final def byte: Int = measure
  }

  private def validate(bytes: Int, unit: String): Int = {
    if (bytes < 0) {
      val exception = new Exception(s"Negative Integer size. $bytes.$unit = $bytes.bytes")
      logger.error(exception.getMessage)
      throw exception
    }

    bytes
  }

  private def validate(bytes: Long, unit: String): Long = {
    if (bytes < 0) {
      val exception = new Exception(s"Negative Long size. $bytes.$unit = $bytes.bytes")
      logger.error(exception.getMessage)
      throw exception
    }

    bytes
  }

  implicit class StorageDoubleImplicits(measure: Double) {

    @inline final def mb: Int =
      validate(bytes = (measure * 1000000).toInt, unit = "mb")

    @inline final def gb: Int =
      validate(bytes = measure.mb * 1000, unit = "gb")

    @inline final def kb: Int =
      validate(bytes = (measure * 1000).toInt, unit = "kb")

    @inline final def mb_long: Long =
      validate(bytes = (measure * 1000000L).toLong, unit = "mb_long")

    @inline final def gb_long: Long =
      validate(bytes = measure.mb_long * 1000L, unit = "gb_long")

    @inline final def kb_long: Long =
      validate(bytes = (measure * 1000L).toLong, unit = "kb_long")
  }

}
