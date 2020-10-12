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

package swaydb.persistent

import swaydb.CommonConfigs
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.MemoryCache.ByteCacheOnly
import swaydb.data.config._
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DefaultConfigs {

  def mmap(): MMAP.On =
    MMAP.On(
      deleteAfterClean =
        OperatingSystem.isWindows,
      forceSave =
        ForceSave.BeforeClean(
          enableBeforeCopy = false,
          enableForReadOnlyMode = false,
          logBenchmark = false
        )
    )

  def sortedKeyIndex(cacheDataBlockOnAccess: Boolean = true): SortedKeyIndex.On =
    SortedKeyIndex.On(
      prefixCompression = PrefixCompression.Off(normaliseIndexForBinarySearch = false),
      enablePositionIndex = true,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compressions = _ => Seq.empty
    )

  def randomSearchIndex(cacheDataBlockOnAccess: Boolean = true): RandomSearchIndex.On =
    RandomSearchIndex.On(
      maxProbe = 1,
      minimumNumberOfKeys = 5,
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
      minimumNumberOfKeys = 10,
      searchSortedIndexDirectly = true,
      indexFormat = IndexFormat.CopyKey,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def mightContainIndex(cacheDataBlockOnAccess: Boolean = true): MightContainIndex.On =
    MightContainIndex.On(
      falsePositiveRate = 0.01,
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
      deleteSegmentsEventually = false,
      pushForward = true,
      mmap = DefaultConfigs.mmap(),
      minSegmentSize = CommonConfigs.segmentSize,
      maxKeyValuesPerSegment = Int.MaxValue,
      fileOpenIOStrategy = IOStrategy.SynchronisedIO(cacheOnAccess = true),
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
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
          name = s"${this.getClass.getName} - MemoryCache Actor",
          delay = 10.seconds,
          ec = ec
        )
    )

  def levelZeroThrottle(meter: LevelZeroMeter): FiniteDuration = {
    val mapsCount = meter.mapsCount

    /**
     * [[CommonConfigs.accelerator]] starts braking at 5 so if the
     * map size comes near to braking do priority compaction to avoid
     * brake from occurring.
     */
    if (mapsCount >= 4)
      Duration.Zero - 365.days //urgent
    else if (mapsCount >= 2)
      1.second
    else
      1.day
  }

  /**
   * The general idea for the following [[Throttle]] functions
   * is that we set the urgency of compaction for each level
   * compared to other levels. The returned [[FiniteDuration]]
   * tells compaction which level's compaction is urgent.
   *
   * The lower the [[FiniteDuration]] the higher it's priority.
   */

  def levelOneThrottle(meter: LevelMeter): Throttle = {
    val segmentsCount = meter.segmentsCount
    val delay = (1 - segmentsCount).seconds
    val segmentsToPush = segmentsCount min 5
    Throttle(delay, segmentsToPush)
  }

  def levelTwoThrottle(meter: LevelMeter): Throttle = {
    val segmentsCount = meter.segmentsCount
    val delay = (3 - segmentsCount).seconds
    val segmentsToPush = segmentsCount min 4
    Throttle(delay, segmentsToPush)
  }

  def levelThreeThrottle(meter: LevelMeter): Throttle = {
    val segmentsCount = meter.segmentsCount
    val delay = (5 - segmentsCount).seconds
    val segmentsToPush = segmentsCount min 3
    Throttle(delay, segmentsToPush)
  }

  def levelFourThrottle(meter: LevelMeter): Throttle = {
    val segmentsCount = meter.segmentsCount
    val delay = (10 - segmentsCount).seconds
    val segmentsToPush = segmentsCount min 2
    Throttle(delay, segmentsToPush)
  }

  def levelFiveThrottle(meter: LevelMeter): Throttle = {
    val segmentsCount = meter.segmentsCount
    val delay = (15 - segmentsCount).seconds
    val segmentsToPush = segmentsCount min 1
    Throttle(delay, segmentsToPush)
  }

  def levelSixThrottle(meter: LevelMeter): Throttle =
    if (meter.requiresCleanUp)
      Throttle(10.seconds, 2)
    else
      Throttle(1.hour, 5)
}
