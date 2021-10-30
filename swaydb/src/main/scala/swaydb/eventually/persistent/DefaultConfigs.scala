/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.eventually.persistent

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.config.MemoryCache.ByteCacheOnly
import swaydb.data.config._
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.StorageUnits._
import swaydb.{ActorConfig, CommonConfigs}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DefaultConfigs {

  //4098 being the default file-system blockSize.
  def logSize: Int = 8.mb

  def segmentSize: Int = 8.mb

  def accelerator: LevelZeroMeter => Accelerator =
    Accelerator.brake(
      increaseLogSizeOnMapCount = 1,
      increaseLogSizeBy = 1,
      maxLogSize = logSize,
      brakeOnMapCount = 6,
      brakeFor = 1.milliseconds,
      releaseRate = 0.01.millisecond,
      logAsWarning = false
    )

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

  def hashIndex(cacheDataBlockOnAccess: Boolean = true): HashIndex.On =
    HashIndex.On(
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
      cacheSegmentBlocksOnCreate = true,
      deleteDelay = CommonConfigs.segmentDeleteDelay,
      //mmap is disabled for eventually persistent databases to give in-memory levels more memory-space.
      mmap = DefaultConfigs.mmap(),
      minSegmentSize = segmentSize,
      initialiseIteratorsInOneSeek = false,
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
      cacheCapacity = 200.mb,
      disableForSearchIO = true,
      actorConfig =
        ActorConfig.TimeLoop(
          name = s"${this.getClass.getName} - MemoryCache TimeLoop Actor",
          delay = 10.seconds,
          ec = ec
        )
    )
}
