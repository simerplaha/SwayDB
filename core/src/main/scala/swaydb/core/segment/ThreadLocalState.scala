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
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.block.{BinarySearchIndexBlock, BloomFilterBlock, HashIndexBlock, SortedIndexBlock, ValuesBlock}
import swaydb.core.segment.format.a.block.reader.UnblockedReader

import scala.beans.BeanProperty

object ThreadLocalState {
  def create(): ThreadLocal[ThreadLocalState] =
    ThreadLocal.withInitial[ThreadLocalState] {
      new Supplier[ThreadLocalState] {
        override def get(): ThreadLocalState =
          ThreadLocalState(None, None, None, None, None)
      }
    }
}

case class ThreadLocalState(@BeanProperty var hashIndexReader: Option[IO.Success[swaydb.Error.Segment, Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]]]],
                            @BeanProperty var bloomFilterReader: Option[IO.Success[swaydb.Error.Segment, Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]]]],
                            @BeanProperty var binarySearchIndexReader: Option[IO.Success[swaydb.Error.Segment, Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]]]],
                            @BeanProperty var valuesReader: Option[IO.Success[swaydb.Error.Segment, Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]]],
                            @BeanProperty var sortedIndexReader: Option[IO.Success[swaydb.Error.Segment, UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]]])
