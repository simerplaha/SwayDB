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

private[swaydb] sealed trait ThreadReadState {
  def getSegmentState(path: Path): ThreadReadState.SegmentStateOptional
  def setSegmentState(path: Path, nextIndexOffset: ThreadReadState.SegmentState): Unit
}

private[swaydb] object ThreadReadState {

  sealed trait SegmentStateOptional extends SomeOrNone[SegmentStateOptional, SegmentState] {
    override def noneS: SegmentStateOptional = SegmentState.Null
  }

  object SegmentState {
    final object Null extends SegmentStateOptional {
      override def isNoneS: Boolean = true
      override def getS: SegmentState = throw new Exception("SegmentState is of type Null")
    }

    def updateOnSuccessSequentialRead(path: Path,
                                      segmentState: SegmentStateOptional,
                                      threadReadState: ThreadReadState,
                                      found: Persistent): Unit =
      if (segmentState.isNoneS)
        createOnSuccessSequentialRead(
          path = path,
          readState = threadReadState,
          found = found
        )
      else
        mutateOnSuccessSequentialRead(
          path = path,
          readState = threadReadState,
          segmentState = segmentState.getS,
          found = found
        )

    /**
     * Sets read state after successful sequential read.
     */

    def createOnSuccessSequentialRead(path: Path,
                                      readState: ThreadReadState,
                                      found: Persistent): Unit = {
      found.unsliceKeys

      val segmentState =
        new ThreadReadState.SegmentState(
          keyValue = found,
          isSequential = true
        )

      readState.setSegmentState(path, segmentState)
    }

    def mutateOnSuccessSequentialRead(path: Path,
                                      readState: ThreadReadState,
                                      segmentState: ThreadReadState.SegmentState,
                                      found: Persistent): Unit = {
      found.unsliceKeys
      val state = segmentState.getS
      //mutate segmentState for next sequential read
      state.keyValue = found
      state.isSequential = true
    }

    /**
     * Sets read state after a random read WITHOUT an existing [[ThreadReadState.SegmentState]] exists.
     */
    def createAfterRandomRead(path: Path,
                              start: PersistentOptional,
                              readState: ThreadReadState,
                              found: PersistentOptional): Unit =

      if (found.isSomeS) {
        val foundKeyValue = found.getS

        foundKeyValue.unsliceKeys

        val segmentState =
          new core.segment.ThreadReadState.SegmentState(
            keyValue = foundKeyValue,
            isSequential = start.isSomeS && foundKeyValue.indexOffset == start.getS.nextIndexOffset
          )

        readState.setSegmentState(path, segmentState)
      }

    /**
     * Sets read state after a random read WITH an existing [[ThreadReadState.SegmentState]] exists.
     */
    def mutateAfterRandomRead(path: Path,
                              readState: ThreadReadState,
                              segmentState: ThreadReadState.SegmentState, //should not be null.
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

  def hashMap(): ThreadReadState =
    new HashMapState(new java.util.HashMap[Path, ThreadReadState.SegmentState]())

  def limitHashMap(maxSize: Int,
                   probe: Int): ThreadReadState =
    new LimitHashMapState(LimitHashMap[Path, ThreadReadState.SegmentState](maxSize, probe))

  def limitHashMap(maxSize: Int): ThreadReadState =
    new LimitHashMapState(LimitHashMap[Path, ThreadReadState.SegmentState](maxSize))

  private class HashMapState(map: java.util.HashMap[Path, ThreadReadState.SegmentState]) extends ThreadReadState {

    def getSegmentState(path: Path): ThreadReadState.SegmentStateOptional = {
      val state = map.get(path)
      if (state == null)
        SegmentState.Null
      else
        state
    }

    def setSegmentState(path: Path, nextIndexOffset: ThreadReadState.SegmentState): Unit =
      map.put(path, nextIndexOffset)
  }

  private class LimitHashMapState(map: LimitHashMap[Path, ThreadReadState.SegmentState]) extends ThreadReadState {

    def getSegmentState(path: Path): ThreadReadState.SegmentStateOptional = {
      val state = map.getOrNull(path)
      if (state == null)
        SegmentState.Null
      else
        state
    }

    def setSegmentState(path: Path, nextIndexOffset: ThreadReadState.SegmentState): Unit =
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
