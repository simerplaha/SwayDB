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

import swaydb.core
import swaydb.core.data.{Persistent, PersistentOptional}
import swaydb.core.util.LimitHashMap
import swaydb.data.util.SomeOrNone

import scala.util.Random

private[swaydb] sealed trait ReadState {
  def getSegmentState(path: Path): ReadState.SegmentStateOptional
  def setSegmentState(path: Path, nextIndexOffset: ReadState.SegmentState): Unit
}

private[swaydb] object ReadState {

  sealed trait SegmentStateOptional extends SomeOrNone[SegmentStateOptional, SegmentState] {
    override def noneS: SegmentStateOptional = SegmentState.Null
  }
  object SegmentState {
    final object Null extends SegmentStateOptional {
      override def isNoneS: Boolean = true
      override def getS: SegmentState = throw new Exception("SegmentState is of type Null")
    }

    /**
     * Sets read state after successful sequential read.
     */

    def createOnSuccessSequentialRead(path: Path,
                                      readState: ReadState,
                                      found: Persistent): Unit = {
      found.unsliceKeys

      val segmentState =
        new ReadState.SegmentState(
          keyValue = found,
          isSequential = true
        )

      readState.setSegmentState(path, segmentState)
    }

    def mutateOnSuccessSequentialRead(path: Path,
                                      readState: ReadState,
                                      segmentState: ReadState.SegmentState,
                                      found: Persistent): Unit = {
      found.unsliceKeys
      val state = segmentState.getS
      //mutate segmentState for next sequential read
      state.keyValue = found
      state.isSequential = true
    }

    /**
     * Sets read state after a random read WITHOUT an existing [[ReadState.SegmentState]] exists.
     */
    def createAfterRandomRead(path: Path,
                              start: PersistentOptional,
                              readState: ReadState,
                              found: PersistentOptional): Unit =

      if (found.isSomeS) {
        val foundKeyValue = found.getS

        foundKeyValue.unsliceKeys

        val segmentState =
          new core.segment.ReadState.SegmentState(
            keyValue = foundKeyValue,
            isSequential = start.isSomeS && foundKeyValue.indexOffset == start.getS.nextIndexOffset
          )

        readState.setSegmentState(path, segmentState)
      }

    /**
     * Sets read state after a random read WITH an existing [[ReadState.SegmentState]] exists.
     */
    def mutateAfterRandomRead(path: Path,
                              readState: ReadState,
                              segmentState: ReadState.SegmentState, //should not be null.
                              found: PersistentOptional): Unit =
      if (found.isSomeS) {
        val foundKeyValue = found.getS
        foundKeyValue.unsliceKeys
        segmentState.isSequential = foundKeyValue.indexOffset == segmentState.keyValue.nextIndexOffset
        segmentState.keyValue = foundKeyValue
      } else {
        segmentState.isSequential = false
      }

  }

  class SegmentState(var keyValue: Persistent,
                     var isSequential: Boolean) extends SegmentStateOptional {
    override def isNoneS: Boolean = false
    override def getS: SegmentState = this
  }

  def hashMap(): ReadState =
    new HashMapState(new java.util.HashMap[Path, ReadState.SegmentState]())

  def limitHashMap(maxSize: Int,
                   probe: Int): ReadState =
    new LimitHashMapState(LimitHashMap[Path, ReadState.SegmentState](maxSize, probe))

  def limitHashMap(maxSize: Int): ReadState =
    new LimitHashMapState(LimitHashMap[Path, ReadState.SegmentState](maxSize))

  private class HashMapState(map: java.util.HashMap[Path, ReadState.SegmentState]) extends ReadState {

    def getSegmentState(path: Path): ReadState.SegmentStateOptional = {
      val state = map.get(path)
      if (state == null)
        SegmentState.Null
      else
        state
    }

    def setSegmentState(path: Path, nextIndexOffset: ReadState.SegmentState): Unit =
      map.put(path, nextIndexOffset)
  }

  private class LimitHashMapState(map: LimitHashMap[Path, ReadState.SegmentState]) extends ReadState {

    def getSegmentState(path: Path): ReadState.SegmentStateOptional = {
      val state = map.getOrNull(path)
      if (state == null)
        SegmentState.Null
      else
        state
    }

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
