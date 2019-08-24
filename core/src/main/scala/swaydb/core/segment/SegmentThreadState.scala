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

import java.util.concurrent.ConcurrentHashMap

import swaydb.core.util.SkipList
import swaydb.core.util.SkipList.MinMaxSkipList
import swaydb.data.order.KeyOrder

import scala.beans.BeanProperty
import scala.reflect.ClassTag
import scala.util.Random

object SegmentThreadState {
  def create[K, V: ClassTag]()(implicit ordering: KeyOrder[K]) =
    new SegmentThreadStates(new ConcurrentHashMap[Long, SegmentThreadState[K, V]]())
}

class SegmentThreadStates[K, V: ClassTag](states: ConcurrentHashMap[Long, SegmentThreadState[K, V]])(implicit val ordering: KeyOrder[K]) {
  def get(): SegmentThreadState[K, V] = {
    val threadId = Thread.currentThread().getId
    val existingState = states.get(threadId)
    if (existingState == null) {
      //todo - could possible copy the state of another thread instead of creating an empty one?
      val newState = new SegmentThreadState[K, V](skipList = SkipList.minMax[K, V](), 0, 0)
      states.put(threadId, newState)
      newState
    } else {
      existingState
    }
  }

  def clear(): Unit =
    states.clear()
}

sealed trait SegmentReadThreadState {
  def isSequentialRead(): Boolean
  def notifySuccessfulSequentialRead(): Unit
}

class SegmentThreadState[K, V](@BeanProperty var skipList: MinMaxSkipList[K, V],
                               var accessCount: Int,
                               var sequentialReadsSuccess: Int) extends SegmentReadThreadState {
  /**
   * Detects if the read is sequential. If it's random then it randomly resets the flags to
   * return sequential read.
   */
  def isSequentialRead(): Boolean = {
    val seqReadFailures = accessCount - sequentialReadsSuccess
    val isRandom = seqReadFailures > 3 // >= 4
    if (isRandom && Random.nextDouble() < 0.01) {
      //reset
      accessCount = 2 //reset to 2 so that at least two more reads occur (>= 4 || > 3) before confirming that current read is still random.
      sequentialReadsSuccess = 0
    }
    accessCount += 1
    !isRandom
  }

  def notifySuccessfulSequentialRead(): Unit =
    sequentialReadsSuccess += 1
}
