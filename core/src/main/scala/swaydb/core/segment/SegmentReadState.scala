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

package swaydb.core.segment

import java.nio.file.Path

import swaydb.core
import swaydb.core.data.{Persistent, PersistentOption}
import swaydb.data.slice.Slice
import swaydb.data.util.{SomeOrNone, TupleOrNone}

protected sealed trait SegmentReadStateOption extends SomeOrNone[SegmentReadStateOption, SegmentReadState] {
  override def noneS: SegmentReadStateOption = SegmentReadState.Null
}

protected object SegmentReadState {

  final case object Null extends SegmentReadStateOption {
    override def isNoneS: Boolean = true
    override def getS: SegmentReadState = throw new Exception("SegmentState is of type Null")
  }

  def updateOnSuccessSequentialRead(path: Path,
                                    forKey: Slice[Byte],
                                    segmentState: SegmentReadStateOption,
                                    threadReadState: ThreadReadState,
                                    found: Persistent): Unit =
    if (segmentState.isNoneS)
      createOnSuccessSequentialRead(
        path = path,
        forKey = forKey,
        readState = threadReadState,
        found = found
      )
    else
      mutateOnSuccessSequentialRead(
        path = path,
        forKey = forKey,
        readState = threadReadState,
        segmentState = segmentState.getS,
        found = found
      )

  /**
   * Sets read state after successful sequential read.
   */

  def createOnSuccessSequentialRead(path: Path,
                                    forKey: Slice[Byte],
                                    readState: ThreadReadState,
                                    found: Persistent): Unit = {
    found.unsliceKeys

    val segmentState =
      new SegmentReadState(
        keyValue = (forKey.unslice(), found),
        lower = TupleOrNone.None,
        isSequential = true
      )

    readState.setSegmentState(path, segmentState)
  }

  def mutateOnSuccessSequentialRead(path: Path,
                                    forKey: Slice[Byte],
                                    readState: ThreadReadState,
                                    segmentState: SegmentReadState,
                                    found: Persistent): Unit = {
    found.unsliceKeys
    val state = segmentState.getS
    //mutate segmentState for next sequential read
    state.keyValue = (forKey.unslice(), found)
    state.isSequential = true
  }

  def updateAfterRandomRead(path: Path,
                            forKey: Slice[Byte],
                            start: PersistentOption,
                            segmentStateOptional: SegmentReadStateOption,
                            threadReadState: ThreadReadState,
                            foundOption: PersistentOption): Unit =
    if (segmentStateOptional.isSomeS)
      SegmentReadState.mutateAfterRandomRead(
        path = path,
        forKey = forKey,
        threadState = threadReadState,
        segmentState = segmentStateOptional.getS,
        foundOption = foundOption
      )
    else
      SegmentReadState.createAfterRandomRead(
        path = path,
        forKey = forKey,
        start = start,
        threadState = threadReadState,
        foundOption = foundOption
      )

  /**
   * Sets read state after a random read WITHOUT an existing [[SegmentReadState]] exists.
   */
  def createAfterRandomRead(path: Path,
                            forKey: Slice[Byte],
                            start: PersistentOption,
                            threadState: ThreadReadState,
                            foundOption: PersistentOption): Unit =

    if (foundOption.isSomeS) {
      val foundKeyValue = foundOption.getS

      foundKeyValue.unsliceKeys

      val segmentState =
        new core.segment.SegmentReadState(
          keyValue = (forKey.unslice(), foundKeyValue),
          lower = TupleOrNone.None,
          isSequential = start.isSomeS && foundKeyValue.indexOffset == start.getS.nextIndexOffset
        )

      threadState.setSegmentState(path, segmentState)
    }

  /**
   * Sets read state after a random read WITH an existing [[SegmentReadState]] exists.
   */
  def mutateAfterRandomRead(path: Path,
                            forKey: Slice[Byte],
                            threadState: ThreadReadState,
                            segmentState: SegmentReadState, //should not be null.
                            foundOption: PersistentOption): Unit =
    if (foundOption.isSomeS) {
      val foundKeyValue = foundOption.getS
      foundKeyValue.unsliceKeys
      segmentState.isSequential = foundKeyValue.indexOffset == segmentState.keyValue._2.nextIndexOffset
      segmentState.keyValue = (forKey.unslice(), foundKeyValue)
    } else {
      segmentState.isSequential = false
    }
}

/**
 * Stores read state of each accessed Segment.
 * This cache is currently managed in [[swaydb.core.Core.readStates]].
 *
 * Both Get and Higher functions mutate [[keyValue]]. But lower
 * can only mutate [[lower]] as it depends on get to fetch
 * the end key-value for faster lower search and should not mutate
 * get's set [[keyValue]].
 */
protected class SegmentReadState(var keyValue: (Slice[Byte], Persistent),
                                 var lower: TupleOrNone[Slice[Byte], Persistent],
                                 var isSequential: Boolean) extends SegmentReadStateOption {
  override def isNoneS: Boolean = false
  override def getS: SegmentReadState = this
}
