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

import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.{SortedIndexBlock, ValuesBlock}
import swaydb.core.util.SkipList
import swaydb.data.order.KeyOrder
import swaydb.{Error, IO}

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.reflect.ClassTag

private[segment] object SegmentThreadState {
  def create[K, V: ClassTag]()(implicit ordering: KeyOrder[K]) =
    new SegmentThreadStates(new ConcurrentHashMap[Long, SegmentThreadState[K, V]]())
}

private[segment] class SegmentThreadStates[K, V: ClassTag](states: ConcurrentHashMap[Long, SegmentThreadState[K, V]])(implicit val ordering: KeyOrder[K]) {
  def get(): SegmentThreadState[K, V] = {
    val threadId = Thread.currentThread().getId
    val existingState = states.get(threadId)
    if (existingState == null) {
      //todo - could possible copy the state of another thread instead of creating an empty one?
      val newState = new SegmentThreadState[K, V](skipList = SkipList.concurrent[K, V](10), None, None, true)
      states.put(threadId, newState)
      newState
    } else {
      existingState
    }
  }

  def clear(): Unit =
    states.clear()
}

private[core] sealed trait SegmentReadThreadState {
  @BooleanBeanProperty var sequentialRead: Boolean
}

object SegmentReadThreadState {
  def empty(seqRead: Boolean) =
    new SegmentReadThreadState {
      @BooleanBeanProperty override var sequentialRead: Boolean = seqRead
    }
}

private[segment] class SegmentThreadState[K, V](val skipList: SkipList.ConcurrentLimit[K, V],
                                                @BeanProperty var sortedIndexReader: Option[IO.Right[Error.Segment, UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]]],
                                                @BeanProperty var valuesReader: Option[IO.Right[Error.Segment, Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]]],
                                                @BooleanBeanProperty var sequentialRead: Boolean) extends SegmentReadThreadState
