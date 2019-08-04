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

import java.util.function.Supplier

import swaydb.IO
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.{SkipList, SkipListValue}

import scala.beans.BeanProperty
import scala.reflect.ClassTag

object SegmentThreadLocalState {
  def create[K, V: ClassTag]()(implicit ordering: Ordering[K]): ThreadLocal[SegmentThreadLocalState[K, V]] =
    ThreadLocal.withInitial[SegmentThreadLocalState[K, V]] {
      new Supplier[SegmentThreadLocalState[K, V]] {
        override def get(): SegmentThreadLocalState[K, V] =
          new SegmentThreadLocalState[K, V](
            hashIndexReader = None,
            bloomFilterReader = None,
            binarySearchIndexReader = None,
            valuesReader = None, sortedIndexReader = None,
            skipList = SkipList.value[K, V]()
          )
      }
    }
}

case class SegmentThreadLocalState[K, V](@BeanProperty var hashIndexReader: Option[IO.Success[swaydb.Error.Segment, Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]]]],
                                         @BeanProperty var bloomFilterReader: Option[IO.Success[swaydb.Error.Segment, Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]]]],
                                         @BeanProperty var binarySearchIndexReader: Option[IO.Success[swaydb.Error.Segment, Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]]]],
                                         @BeanProperty var valuesReader: Option[IO.Success[swaydb.Error.Segment, Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]]],
                                         @BeanProperty var sortedIndexReader: Option[IO.Success[swaydb.Error.Segment, UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]]],
                                         @BeanProperty var skipList: SkipListValue[K, V])
