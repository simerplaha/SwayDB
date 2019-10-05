/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.segment

import java.nio.file.Path

import swaydb.core.util.LimitHashMap

import scala.util.Random

private[swaydb] sealed trait ReadState {
  def isSequential(path: Path): Boolean
  def setSequential(path: Path, isSequential: Boolean): Unit
}

private[swaydb] object ReadState {

  def hashMap(): ReadState =
    new HashMapState(new java.util.HashMap[Path, Boolean]())

  def limitHashMap(maxSize: Int,
                   probe: Int): ReadState =
    new LimitHashMapState(LimitHashMap[Path, Boolean](maxSize, probe))

  def limitHashMap(maxSize: Int): ReadState =
    new LimitHashMapState(LimitHashMap[Path, Boolean](maxSize))

  private class HashMapState(map: java.util.HashMap[Path, Boolean]) extends ReadState {

    def isSequential(path: Path): Boolean = {
      val isSeq = map.get(path)
      isSeq == null || isSeq
    }

    def setSequential(path: Path, isSequential: Boolean): Unit =
      map.put(path, isSequential)
  }

  private class LimitHashMapState(map: LimitHashMap[Path, Boolean]) extends ReadState {

    def isSequential(path: Path): Boolean =
      map.get(path).forall(_ == true)

    def setSequential(path: Path, isSequential: Boolean): Unit =
      map.put(path, isSequential)

    override def toString: String =
      map.toString
  }

  def random: ReadState =
    if (scala.util.Random.nextBoolean())
      ReadState.hashMap()
    else if (scala.util.Random.nextBoolean())
      ReadState.limitHashMap(10, Random.nextInt(10))
    else
      ReadState.limitHashMap(20)
}
