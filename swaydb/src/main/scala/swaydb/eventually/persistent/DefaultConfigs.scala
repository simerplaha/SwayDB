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

package swaydb.eventually.persistent

import swaydb.SwayDB
import swaydb.data.config.MemoryCache.ByteCacheOnly
import swaydb.data.config._
import swaydb.data.util.StorageUnits._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

object DefaultConfigs {

  implicit lazy val sweeperEC: ExecutionContext = SwayDB.sweeperExecutionContext

  def sortedKeyIndex(cacheDataBlockOnAccess: Boolean = true): SortedKeyIndex.Enable =
    SortedKeyIndex.Enable(
      prefixCompression = PrefixCompression.Disable(normaliseIndexForBinarySearch = false),
      enablePositionIndex = true,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compressions = _ => Seq.empty
    )

  def randomKeyIndex(cacheDataBlockOnAccess: Boolean = true): RandomKeyIndex.Enable =
    RandomKeyIndex.Enable(
      maxProbe = 1,
      minimumNumberOfKeys = 2,
      minimumNumberOfHits = 2,
      indexFormat = IndexFormat.Reference,
      allocateSpace = _.requiredSpace,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def binarySearchIndex(cacheDataBlockOnAccess: Boolean = true): BinarySearchIndex.FullIndex =
    BinarySearchIndex.FullIndex(
      minimumNumberOfKeys = 5,
      indexFormat = IndexFormat.CopyKey,
      searchSortedIndexDirectly = true,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def mightContainKeyIndex(cacheDataBlockOnAccess: Boolean = true): MightContainIndex.Enable =
    MightContainIndex.Enable(
      falsePositiveRate = 0.001,
      minimumNumberOfKeys = 10,
      updateMaxProbe = optimalMaxProbe => 1,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def valuesConfig(cacheDataBlockOnAccess: Boolean = false): ValuesConfig =
    ValuesConfig(
      compressDuplicateValues = true,
      compressDuplicateRangeValues = true,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def segmentConfig(cacheDataBlockOnAccess: Boolean = false): SegmentConfig =
    SegmentConfig(
      cacheSegmentBlocksOnCreate = true,
      deleteSegmentsEventually = true,
      pushForward = false,
      mmap = MMAP.Disabled,
      minSegmentSize = 8.mb,
      maxKeyValuesPerSegment = Int.MaxValue,
      fileOpenIOStrategy = IOStrategy.SynchronisedIO(cacheOnAccess = true),
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def fileCache(implicit ec: ExecutionContext = sweeperEC): FileCache.Enable =
    FileCache.Enable(
      maxOpen = 500,
      actorConfig =
        ActorConfig.TimeLoop(
          name = s"${this.getClass.getName} - FileCache TimeLoop Actor",
          delay = 10.seconds,
          ec = ec
        )
    )

  def memoryCache(implicit ec: ExecutionContext = sweeperEC): MemoryCache.ByteCacheOnly =
    ByteCacheOnly(
      minIOSeekSize = 4098,
      skipBlockCacheSeekSize = 4098 * 10,
      cacheCapacity = 1.gb,
      actorConfig =
        ActorConfig.TimeLoop(
          name = s"${this.getClass.getName} - MemoryCache TimeLoop Actor",
          delay = 10.seconds,
          ec = ec
        )
    )
}
