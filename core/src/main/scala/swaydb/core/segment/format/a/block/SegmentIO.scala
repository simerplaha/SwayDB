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

package swaydb.core.segment.format.a.block

import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.data.config.{IOAction, IOStrategy}

object SegmentIO {

  def defaultSynchronisedStoredIfCompressed =
    new SegmentIO(
      segmentBlockIO = IOStrategy.synchronisedStoredIfCompressed,
      hashIndexBlockIO = IOStrategy.synchronisedStoredIfCompressed,
      bloomFilterBlockIO = IOStrategy.synchronisedStoredIfCompressed,
      binarySearchIndexBlockIO = IOStrategy.synchronisedStoredIfCompressed,
      sortedIndexBlockIO = IOStrategy.synchronisedStoredIfCompressed,
      valuesBlockIO = IOStrategy.synchronisedStoredIfCompressed,
      segmentFooterBlockIO = IOStrategy.synchronisedStoredIfCompressed
    )

  def defaultConcurrentStoredIfCompressed =
    new SegmentIO(
      segmentBlockIO = IOStrategy.concurrentStoredIfCompressed,
      hashIndexBlockIO = IOStrategy.concurrentStoredIfCompressed,
      bloomFilterBlockIO = IOStrategy.concurrentStoredIfCompressed,
      binarySearchIndexBlockIO = IOStrategy.concurrentStoredIfCompressed,
      sortedIndexBlockIO = IOStrategy.concurrentStoredIfCompressed,
      valuesBlockIO = IOStrategy.concurrentStoredIfCompressed,
      segmentFooterBlockIO = IOStrategy.concurrentStoredIfCompressed
    )

  def defaultConcurrentStored =
    new SegmentIO(
      segmentBlockIO = IOStrategy.concurrentStored,
      hashIndexBlockIO = IOStrategy.concurrentStored,
      bloomFilterBlockIO = IOStrategy.concurrentStored,
      binarySearchIndexBlockIO = IOStrategy.concurrentStored,
      sortedIndexBlockIO = IOStrategy.concurrentStored,
      valuesBlockIO = IOStrategy.concurrentStored,
      segmentFooterBlockIO = IOStrategy.concurrentStored
    )

  def defaultSynchronisedStored =
    new SegmentIO(
      segmentBlockIO = IOStrategy.synchronisedStored,
      hashIndexBlockIO = IOStrategy.synchronisedStored,
      bloomFilterBlockIO = IOStrategy.synchronisedStored,
      binarySearchIndexBlockIO = IOStrategy.synchronisedStored,
      sortedIndexBlockIO = IOStrategy.synchronisedStored,
      valuesBlockIO = IOStrategy.synchronisedStored,
      segmentFooterBlockIO = IOStrategy.synchronisedStored
    )

  def apply(bloomFilterConfig: BloomFilterBlock.Config,
            hashIndexConfig: HashIndexBlock.Config,
            binarySearchIndexConfig: BinarySearchIndexBlock.Config,
            sortedIndexConfig: SortedIndexBlock.Config,
            valuesConfig: ValuesBlock.Config,
            segmentConfig: SegmentBlock.Config): SegmentIO =
    new SegmentIO(
      segmentBlockIO = segmentConfig.ioStrategy,
      hashIndexBlockIO = hashIndexConfig.ioStrategy,
      bloomFilterBlockIO = bloomFilterConfig.ioStrategy,
      binarySearchIndexBlockIO = binarySearchIndexConfig.ioStrategy,
      sortedIndexBlockIO = sortedIndexConfig.ioStrategy,
      valuesBlockIO = valuesConfig.ioStrategy,
      segmentFooterBlockIO = segmentConfig.ioStrategy
    )
}

private[core] case class SegmentIO(segmentBlockIO: IOAction => IOStrategy,
                                   hashIndexBlockIO: IOAction => IOStrategy,
                                   bloomFilterBlockIO: IOAction => IOStrategy,
                                   binarySearchIndexBlockIO: IOAction => IOStrategy,
                                   sortedIndexBlockIO: IOAction => IOStrategy,
                                   valuesBlockIO: IOAction => IOStrategy,
                                   segmentFooterBlockIO: IOAction => IOStrategy)
