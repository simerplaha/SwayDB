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

import swaydb.data.config.{IOAction, IOStrategy}

object SegmentIO {

  def defaultSynchronisedStoredIfCompressed =
    new SegmentIO(
      segmentBlockIO = IOStrategy.defaultSynchronisedStoredIfCompressed,
      hashIndexBlockIO = IOStrategy.defaultSynchronisedStoredIfCompressed,
      bloomFilterBlockIO = IOStrategy.defaultSynchronisedStoredIfCompressed,
      binarySearchIndexBlockIO = IOStrategy.defaultSynchronisedStoredIfCompressed,
      sortedIndexBlockIO = IOStrategy.defaultSynchronisedStoredIfCompressed,
      valuesBlockIO = IOStrategy.defaultSynchronisedStoredIfCompressed,
      segmentFooterBlockIO = IOStrategy.defaultSynchronisedStoredIfCompressed
    )

  def defaultSynchronisedStored =
    new SegmentIO(
      segmentBlockIO = IOStrategy.defaultSynchronisedStored,
      hashIndexBlockIO = IOStrategy.defaultSynchronisedStored,
      bloomFilterBlockIO = IOStrategy.defaultSynchronisedStored,
      binarySearchIndexBlockIO = IOStrategy.defaultSynchronisedStored,
      sortedIndexBlockIO = IOStrategy.defaultSynchronisedStored,
      valuesBlockIO = IOStrategy.defaultSynchronisedStored,
      segmentFooterBlockIO = IOStrategy.defaultSynchronisedStored
    )

  def apply(bloomFilterConfig: BloomFilterBlock.Config,
            hashIndexConfig: HashIndexBlock.Config,
            binarySearchIndexConfig: BinarySearchIndexBlock.Config,
            sortedIndexConfig: SortedIndexBlock.Config,
            valuesConfig: ValuesBlock.Config,
            segmentConfig: SegmentBlock.Config): SegmentIO =
    new SegmentIO(
      segmentBlockIO = segmentConfig.blockIO,
      hashIndexBlockIO = hashIndexConfig.blockIO,
      bloomFilterBlockIO = bloomFilterConfig.blockIO,
      binarySearchIndexBlockIO = binarySearchIndexConfig.blockIO,
      sortedIndexBlockIO = sortedIndexConfig.blockIO,
      valuesBlockIO = valuesConfig.blockIO,
      segmentFooterBlockIO = segmentConfig.blockIO
    )
}

private[core] case class SegmentIO(segmentBlockIO: IOAction => IOStrategy,
                                   hashIndexBlockIO: IOAction => IOStrategy,
                                   bloomFilterBlockIO: IOAction => IOStrategy,
                                   binarySearchIndexBlockIO: IOAction => IOStrategy,
                                   sortedIndexBlockIO: IOAction => IOStrategy,
                                   valuesBlockIO: IOAction => IOStrategy,
                                   segmentFooterBlockIO: IOAction => IOStrategy)
