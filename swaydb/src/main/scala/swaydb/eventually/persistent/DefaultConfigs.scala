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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.eventually.persistent

import swaydb.CommonConfigs
import swaydb.data.config.MemoryCache.ByteCacheOnly
import swaydb.data.config._
import swaydb.data.util.StorageUnits._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DefaultConfigs {

  def mmap(): MMAP.Off =
    MMAP.Off(
      forceSave =
        ForceSave.BeforeClose(
          enableBeforeCopy = false,
          enableForReadOnlyMode = false,
          logBenchmark = false
        )
    )

  def sortedKeyIndex(cacheDataBlockOnAccess: Boolean = true): SortedKeyIndex.On =
    SortedKeyIndex.On(
      prefixCompression = PrefixCompression.Off(normaliseIndexForBinarySearch = false),
      enablePositionIndex = true,
      optimiseForReverseIteration = true,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO.cached
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compressions = _ => Seq.empty
    )

  def randomSearchIndex(cacheDataBlockOnAccess: Boolean = true): RandomSearchIndex.On =
    RandomSearchIndex.On(
      maxProbe = 1,
      minimumNumberOfKeys = 2,
      minimumNumberOfHits = 2,
      indexFormat = IndexFormat.Reference,
      allocateSpace = _.requiredSpace,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO.cached
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
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO.cached
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def mightContainIndex(cacheDataBlockOnAccess: Boolean = true): MightContainIndex.On =
    MightContainIndex.On(
      falsePositiveRate = 0.001,
      minimumNumberOfKeys = 10,
      updateMaxProbe = optimalMaxProbe => 1,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO.cached
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def valuesConfig(cacheDataBlockOnAccess: Boolean = false): ValuesConfig =
    ValuesConfig(
      compressDuplicateValues = true,
      compressDuplicateRangeValues = true,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO.cached
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def segmentConfig(cacheDataBlockOnAccess: Boolean = false): SegmentConfig =
    SegmentConfig(
      cacheSegmentBlocksOnCreate = true,
      deleteDelay = CommonConfigs.segmentDeleteDelay,
      pushForward = PushForwardStrategy.Off,
      //mmap is disabled for eventually persistent databases to give in-memory levels more memory-space.
      mmap = DefaultConfigs.mmap(),
      minSegmentSize = CommonConfigs.segmentSize,
      segmentFormat = SegmentFormat.Flattened,
      fileOpenIOStrategy = IOStrategy.SynchronisedIO.cached,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO.cached
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def fileCache(implicit ec: ExecutionContext): FileCache.On =
    FileCache.On(
      maxOpen = 1000,
      actorConfig =
        ActorConfig.TimeLoop(
          name = s"${this.getClass.getName} - FileCache TimeLoop Actor",
          delay = 10.seconds,
          ec = ec
        )
    )

  def memoryCache(implicit ec: ExecutionContext): MemoryCache.ByteCacheOnly =
    ByteCacheOnly(
      minIOSeekSize = 4096,
      skipBlockCacheSeekSize = 4096 * 10,
      cacheCapacity = 1.gb,
      actorConfig =
        ActorConfig.TimeLoop(
          name = s"${this.getClass.getName} - MemoryCache TimeLoop Actor",
          delay = 10.seconds,
          ec = ec
        )
    )
}
