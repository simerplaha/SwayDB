/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
import swaydb.data.util.SomeOrNone

sealed trait SegmentReadStateOptional extends SomeOrNone[SegmentReadStateOptional, SegmentReadState] {
  override def noneS: SegmentReadStateOptional = SegmentReadState.Null
}

object SegmentReadState {
  final object Null extends SegmentReadStateOptional {
    override def isNoneS: Boolean = true
    override def getS: SegmentReadState = throw new Exception("SegmentState is of type Null")
  }

  def updateOnSuccessSequentialRead(path: Path,
                                    segmentState: SegmentReadStateOptional,
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
      new SegmentReadState(
        keyValue = found,
        lowerKeyValue = Persistent.Null,
        isSequential = true
      )

    readState.setSegmentState(path, segmentState)
  }

  def mutateOnSuccessSequentialRead(path: Path,
                                    readState: ThreadReadState,
                                    segmentState: SegmentReadState,
                                    found: Persistent): Unit = {
    found.unsliceKeys
    val state = segmentState.getS
    //mutate segmentState for next sequential read
    state.keyValue = found
    state.isSequential = true
  }

  def updateAfterRandomRead(path: Path,
                            start: PersistentOptional,
                            segmentStateOptional: SegmentReadStateOptional,
                            threadReadState: ThreadReadState,
                            foundOption: PersistentOptional): Unit =
    if (segmentStateOptional.isSomeS)
      SegmentReadState.mutateAfterRandomRead(
        path = path,
        threadState = threadReadState,
        segmentState = segmentStateOptional.getS,
        foundOption = foundOption
      )
    else
      SegmentReadState.createAfterRandomRead(
        path = path,
        threadState = threadReadState,
        start = start,
        foundOption = foundOption
      )

  /**
   * Sets read state after a random read WITHOUT an existing [[SegmentReadState]] exists.
   */
  def createAfterRandomRead(path: Path,
                            start: PersistentOptional,
                            threadState: ThreadReadState,
                            foundOption: PersistentOptional): Unit =

    if (foundOption.isSomeS) {
      val foundKeyValue = foundOption.getS

      foundKeyValue.unsliceKeys

      val segmentState =
        new core.segment.SegmentReadState(
          keyValue = foundKeyValue,
          lowerKeyValue = Persistent.Null,
          isSequential = start.isSomeS && foundKeyValue.indexOffset == start.getS.nextIndexOffset
        )

      threadState.setSegmentState(path, segmentState)
    }

  /**
   * Sets read state after a random read WITH an existing [[SegmentReadState]] exists.
   */
  def mutateAfterRandomRead(path: Path,
                            threadState: ThreadReadState,
                            segmentState: SegmentReadState, //should not be null.
                            foundOption: PersistentOptional): Unit =
    if (foundOption.isSomeS) {
      val foundKeyValue = foundOption.getS
      foundKeyValue.unsliceKeys
      segmentState.isSequential = foundKeyValue.indexOffset == segmentState.keyValue.nextIndexOffset
      segmentState.keyValue = foundKeyValue
    } else {
      segmentState.isSequential = false
    }
}

/**
 * Both Get and Higher functions mutate [[keyValue]]. But lower
 * can only mutate [[lowerKeyValue]] as it depends on get to fetch
 * the end key-value for faster lower search and should not mutate
 * get's set [[keyValue]].
 */
class SegmentReadState(var keyValue: Persistent,
                       var lowerKeyValue: PersistentOptional,
                       var isSequential: Boolean) extends SegmentReadStateOptional {
  override def isNoneS: Boolean = false
  override def getS: SegmentReadState = this
}