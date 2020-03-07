/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

import java.nio.file.{Path, Paths}

import swaydb.data.config.Dir

package object swaydb {

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

  implicit class StorageDoubleImplicits(measure: Double) {

    @inline final def mb: Int = (measure * 1000000).toInt

    @inline final def gb: Int = measure.mb * 1000

    @inline final def kb: Int = measure.mb * 1000
  }

}