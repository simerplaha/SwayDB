/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.data.config.MemoryCache.ByteCacheOnly
import swaydb.data.config._
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.OperatingSystem
import swaydb.utils.StorageUnits._
import swaydb.{ActorConfig, CommonConfigs}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DefaultConfigs {

  //4098 being the default file-system blockSize.
  def mapSize: Int = 64.mb

  def accelerator: LevelZeroMeter => Accelerator =
    Accelerator.brake(
      increaseMapSizeOnMapCount = 3,
      increaseMapSizeBy = 2,
      maxMapSize = 16.mb,
      brakeOnMapCount = 10,
      brakeFor = 1.milliseconds,
      releaseRate = 0.01.millisecond,
      logAsWarning = false
    )

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

  def sortedKeyIndex(cacheDataBlockOnAccess: Boolean = false): SortedKeyIndex.On =
    SortedKeyIndex.On(
      prefixCompression = PrefixCompression.Off(normaliseIndexForBinarySearch = false),
      //      prefixCompression =
      //        PrefixCompression.On(
      //          keysOnly = true,
      //          interval = PrefixCompression.Interval.ResetCompressionAt(4)
      //        ),
      enablePositionIndex = true,
      optimiseForReverseIteration = true,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO.cached
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compressions = _ => Seq.empty
    )

  def randomSearchIndex(cacheDataBlockOnAccess: Boolean = false): RandomSearchIndex.On =
    RandomSearchIndex.On(
      maxProbe = 5,
      minimumNumberOfKeys = 5,
      minimumNumberOfHits = 2,
      indexFormat = IndexFormat.Reference,
      allocateSpace = _.requiredSpace,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO.cached
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def binarySearchIndex(cacheDataBlockOnAccess: Boolean = false): BinarySearchIndex.FullIndex =
    BinarySearchIndex.FullIndex(
      minimumNumberOfKeys = 10,
      searchSortedIndexDirectly = true,
      indexFormat = IndexFormat.CopyKey,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO.cached
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def mightContainIndex(cacheDataBlockOnAccess: Boolean = false): MightContainIndex.On =
    MightContainIndex.On(
      falsePositiveRate = 0.001,
      minimumNumberOfKeys = 10,
      updateMaxProbe = (optimalMaxProbe: Int) => optimalMaxProbe,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO.cached
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def valuesConfig(cacheDataBlockOnAccess: Boolean = false): ValuesConfig =
    ValuesConfig(
      compressDuplicateValues = false,
      compressDuplicateRangeValues = false,
      blockIOStrategy = {
        case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO.cached
        case action: IOAction.DecompressAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed || cacheDataBlockOnAccess)
      },
      compression = _ => Seq.empty
    )

  def segmentConfig(cacheDataBlockOnAccess: Boolean = false): SegmentConfig =
    SegmentConfig(
      cacheSegmentBlocksOnCreate = false,
      deleteDelay = CommonConfigs.segmentDeleteDelay,
      mmap = MMAP.Off(forceSave = ForceSave.BeforeClose(enableBeforeCopy = false, enableForReadOnlyMode = false, logBenchmark = false)),
      minSegmentSize = 18.mb,
      //      segmentFormat = SegmentFormat.Flattened,
      segmentFormat = SegmentFormat.Grouped(count = 10000, enableRootHashIndex = false, segmentRefCacheLife = SegmentRefCacheLife.Temporary),
      fileOpenIOStrategy = IOStrategy.SynchronisedIO(cacheOnAccess = true),
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
      cacheCapacity = 300.mb_long,
      disableForSearchIO = false,
      actorConfig =
        ActorConfig.TimeLoop(
          name = s"${this.getClass.getName} - MemoryCache Actor",
          delay = 10.seconds,
          ec = ec
        )
    )

  def levelZeroThrottle(meter: LevelZeroMeter): LevelZeroThrottle = {
    val count = meter.mapsCount
    //when there are more than 4 maps/logs in LevelZero
    //then give LevelZero highest priority.
    //This will compact all LevelZero maps at once.

    val delay =
      if (count >= 4)
        -count.seconds
      else
        count.seconds //else give it some delay

    LevelZeroThrottle(
      compactionDelay = delay,
      mapsToCompact = 4
    )
  }

  /**
   * The general idea for the following [[LevelThrottle]] functions
   * is that we set the urgency of compaction for each level
   * compared to other levels. The returned [[FiniteDuration]]
   * tells compaction which level's compaction is urgent.
   *
   * The lower the [[FiniteDuration]] the higher it's priority.
   */

  val idle = LevelThrottle(compactionDelay = 365.day, compactDataSize = 0)

  @inline def calculateThrottle(maxLevelSize: Long, meter: LevelMeter) = {
    val levelSize = meter.levelSize
    val delay = ((maxLevelSize.toDouble / levelSize) * 100D) - 100
    if (delay == Double.PositiveInfinity)
      idle
    else
      LevelThrottle(
        compactionDelay = delay.seconds,
        compactDataSize = levelSize - maxLevelSize
      )
  }

  val levelOneSize = 100.mb

  def levelOneThrottle(meter: LevelMeter): LevelThrottle =
    calculateThrottle(maxLevelSize = levelOneSize, meter = meter)

  def levelTwoThrottle(meter: LevelMeter): LevelThrottle =
    calculateThrottle(maxLevelSize = levelOneSize * 10, meter = meter)

  def levelThreeThrottle(meter: LevelMeter): LevelThrottle =
    calculateThrottle(maxLevelSize = levelOneSize * 100, meter = meter)

  def levelFourThrottle(meter: LevelMeter): LevelThrottle =
    calculateThrottle(maxLevelSize = levelOneSize * 1000, meter = meter)

  def levelFiveThrottle(meter: LevelMeter): LevelThrottle =
    calculateThrottle(maxLevelSize = levelOneSize * 10000, meter = meter)

  def levelSixThrottle(meter: LevelMeter): LevelThrottle =
    if (meter.requiresCleanUp)
      LevelThrottle(10.seconds, 1)
    else
      LevelThrottle(1.hour, 1)
}
