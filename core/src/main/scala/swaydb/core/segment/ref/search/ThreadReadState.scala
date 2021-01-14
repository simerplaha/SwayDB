/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.ref.search

import swaydb.core.util.LimitHashMap

import java.nio.file.Path
import scala.util.Random

private[swaydb] sealed trait ThreadReadState {
  def getSegmentState(path: Path): SegmentReadStateOption
  def setSegmentState(path: Path, nextIndexOffset: SegmentReadState): Unit
}

private[swaydb] object ThreadReadState {

  def hashMap(): ThreadReadState =
    new HashMapState(new java.util.HashMap[Path, SegmentReadState]())

  def limitHashMap(maxSize: Int,
                   probe: Int): ThreadReadState =
    new LimitHashMapState(LimitHashMap[Path, SegmentReadState](maxSize, probe))

  def limitHashMap(maxSize: Int): ThreadReadState =
    new LimitHashMapState(LimitHashMap[Path, SegmentReadState](maxSize))

  private class HashMapState(map: java.util.HashMap[Path, SegmentReadState]) extends ThreadReadState {

    def getSegmentState(path: Path): SegmentReadStateOption = {
      val state = map.get(path)
      if (state == null)
        SegmentReadState.Null
      else
        state
    }

    def setSegmentState(path: Path, nextIndexOffset: SegmentReadState): Unit =
      map.put(path, nextIndexOffset)
  }

  private class LimitHashMapState(map: LimitHashMap[Path, SegmentReadState]) extends ThreadReadState {

    def getSegmentState(path: Path): SegmentReadStateOption = {
      val state = map.getOrNull(path)
      if (state == null)
        SegmentReadState.Null
      else
        state
    }

    def setSegmentState(path: Path, nextIndexOffset: SegmentReadState): Unit =
      map.put(path, nextIndexOffset)

    override def toString: String =
      map.toString
  }

  def random: ThreadReadState =
    if (scala.util.Random.nextBoolean())
      ThreadReadState.hashMap()
    else if (scala.util.Random.nextBoolean())
      ThreadReadState.limitHashMap(10, Random.nextInt(10))
    else
      ThreadReadState.limitHashMap(20)
}
