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
  def getSegmentStateOrNull(path: Path): ReadState.SegmentState
  def setSegmentState(path: Path, nextIndexOffset: ReadState.SegmentState): Unit
}

private[swaydb] object ReadState {

  class SegmentState(var nextIndexOffset: Int,
                     var nextKeySizeOrNull: Int,
                     var isSequential: Boolean)

  def hashMap(): ReadState =
    new HashMapState(new java.util.HashMap[Path, ReadState.SegmentState]())

  def limitHashMap(maxSize: Int,
                   probe: Int): ReadState =
    new LimitHashMapState(LimitHashMap[Path, ReadState.SegmentState](maxSize, probe))

  def limitHashMap(maxSize: Int): ReadState =
    new LimitHashMapState(LimitHashMap[Path, ReadState.SegmentState](maxSize))

  private class HashMapState(map: java.util.HashMap[Path, ReadState.SegmentState]) extends ReadState {

    def getSegmentStateOrNull(path: Path): ReadState.SegmentState =
      map.get(path)

    def setSegmentState(path: Path, nextIndexOffset: ReadState.SegmentState): Unit =
      map.put(path, nextIndexOffset)
  }

  private class LimitHashMapState(map: LimitHashMap[Path, ReadState.SegmentState]) extends ReadState {

    def getSegmentStateOrNull(path: Path): ReadState.SegmentState =
      map.getOrNull(path)

    def setSegmentState(path: Path, nextIndexOffset: ReadState.SegmentState): Unit =
      map.put(path, nextIndexOffset)

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
