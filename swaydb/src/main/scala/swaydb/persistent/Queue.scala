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
 */

package swaydb.persistent

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{Bag, Error, IO}

import scala.concurrent.duration.FiniteDuration

object Queue extends LazyLogging {

  /**
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[A](dir: Path,
               mapSize: Int = 4.mb,
               mmapMaps: Boolean = true,
               recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
               mmapAppendix: Boolean = true,
               appendixFlushCheckpointSize: Int = 2.mb,
               otherDirs: Seq[Dir] = Seq.empty,
               cacheKeyValueIds: Boolean = true,
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
               levelSixThrottle: LevelMeter => Throttle = DefaultConfigs.levelSixThrottle)(implicit serializer: Serializer[A]): IO[Error.Boot, swaydb.Queue[A]] = {

    implicit val longSerializer: Serializer[Long] =
      new Serializer[Long] {
        override def write(data: Long): Slice[Byte] =
          Slice.writeUnsignedLong(data)

        override def read(data: Slice[Byte]): Long =
          data.readUnsignedLong()
      }

    implicit val keyOrder: Right[Nothing, KeyOrder[Long]] =
      Right {
        new KeyOrder[Long] {
          override def comparableKey(data: Long): Long =
            data

          override def compare(x: Long, y: Long): Int =
            x compareTo y
        }
      }

    MapSet[Long, A, Nothing, Bag.Less](
      dir = dir,
      mapSize = mapSize,
      mmapMaps = mmapMaps,
      recoveryMode = recoveryMode,
      mmapAppendix = mmapAppendix,
      appendixFlushCheckpointSize = appendixFlushCheckpointSize,
      otherDirs = otherDirs,
      cacheKeyValueIds = cacheKeyValueIds,
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
    ) map {
      map =>
        val first: Long =
          map.headOption match {
            case Some((first, _)) =>
              first

            case None =>
              0
          }

        val last: Long =
          map.lastOption match {
            case Some((used, _)) =>
              used + 1

            case None =>
              0
          }

        swaydb.Queue(
          map = map,
          pushIds = new AtomicLong(last),
          popIds = new AtomicLong(first)
        )
    }
  }
}
