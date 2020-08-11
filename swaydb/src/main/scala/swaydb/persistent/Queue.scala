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

package swaydb.persistent

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.LazyLogging
import swaydb.Bag
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success, Try}

object Queue extends LazyLogging {

  /**
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[A, BAG[_]](dir: Path,
                       mapSize: Int = 4.mb,
                       mmapMaps: MMAP.Map = MMAP.Enabled(OperatingSystem.isWindows),
                       recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                       mmapAppendix: MMAP.Map = MMAP.Enabled(OperatingSystem.isWindows),
                       appendixFlushCheckpointSize: Int = 2.mb,
                       otherDirs: Seq[Dir] = Seq.empty,
                       cacheKeyValueIds: Boolean = true,
                       shutdownTimeout: FiniteDuration = 30.seconds,
                       acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes(),
                       threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                       sortedKeyIndex: SortedKeyIndex = DefaultConfigs.sortedKeyIndex(),
                       randomKeyIndex: RandomKeyIndex = DefaultConfigs.randomKeyIndex(),
                       binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                       mightContainKeyIndex: MightContainIndex = DefaultConfigs.mightContainKeyIndex(),
                       valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                       segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                       fileCache: FileCache.Enable = DefaultConfigs.fileCache(),
                       memoryCache: MemoryCache = DefaultConfigs.memoryCache(),
                       levelZeroThrottle: LevelZeroMeter => FiniteDuration = DefaultConfigs.levelZeroThrottle,
                       levelOneThrottle: LevelMeter => Throttle = DefaultConfigs.levelOneThrottle,
                       levelTwoThrottle: LevelMeter => Throttle = DefaultConfigs.levelTwoThrottle,
                       levelThreeThrottle: LevelMeter => Throttle = DefaultConfigs.levelThreeThrottle,
                       levelFourThrottle: LevelMeter => Throttle = DefaultConfigs.levelFourThrottle,
                       levelFiveThrottle: LevelMeter => Throttle = DefaultConfigs.levelFiveThrottle,
                       levelSixThrottle: LevelMeter => Throttle = DefaultConfigs.levelSixThrottle)(implicit serializer: Serializer[A],
                                                                                                   bag: Bag[BAG]): BAG[swaydb.Queue[A]] =
    bag.suspend {
      implicit val queueSerialiser: Serializer[(Long, A)] =
        swaydb.Queue.serialiser[A](serializer)

      implicit val keyOrder: KeyOrder[Slice[Byte]] =
        swaydb.Queue.ordering

      Try {
        Set[(Long, A), Nothing, Bag.Less](
          dir = dir,
          mapSize = mapSize,
          mmapMaps = mmapMaps,
          recoveryMode = recoveryMode,
          mmapAppendix = mmapAppendix,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          otherDirs = otherDirs,
          cacheKeyValueIds = cacheKeyValueIds,
          shutdownTimeout = shutdownTimeout,
          acceleration = acceleration,
          threadStateCache = threadStateCache,
          sortedKeyIndex = sortedKeyIndex,
          randomKeyIndex = randomKeyIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainKeyIndex = mightContainKeyIndex,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          fileCache = fileCache,
          memoryCache = memoryCache,
          levelZeroThrottle = levelZeroThrottle,
          levelOneThrottle = levelOneThrottle,
          levelTwoThrottle = levelTwoThrottle,
          levelThreeThrottle = levelThreeThrottle,
          levelFourThrottle = levelFourThrottle,
          levelFiveThrottle = levelFiveThrottle,
          levelSixThrottle = levelSixThrottle
        )
      } match {
        case Success(set) =>
          val first: Long =
            set.headOption match {
              case Some((first, _)) =>
                first

              case None =>
                0
            }

          val last: Long =
            set.lastOption match {
              case Some((used, _)) =>
                used + 1

              case None =>
                0
            }

          val queue =
            swaydb.Queue(
              set = set,
              pushIds = new AtomicLong(last),
              popIds = new AtomicLong(first)
            )

          bag.success(queue)

        case Failure(exception) =>
          bag.failure(exception)
      }
    }
}
