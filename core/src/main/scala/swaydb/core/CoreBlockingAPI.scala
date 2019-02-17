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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Deadline, FiniteDuration}
import swaydb.core.data.KeyValue._
import swaydb.core.data.Memory
import swaydb.core.function.FunctionStore
import swaydb.core.level.zero.LevelZero
import swaydb.core.map.MapEntry
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.config.{LevelZeroConfig, SwayDBConfig}
import swaydb.data.io.IO
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

private[swaydb] object CoreBlockingAPI {

  def apply(config: SwayDBConfig,
            maxOpenSegments: Int,
            cacheSize: Long,
            cacheCheckDelay: FiniteDuration,
            segmentsOpenCheckDelay: FiniteDuration)(implicit ec: ExecutionContext,
                                                    keyOrder: KeyOrder[Slice[Byte]],
                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                    functionStore: FunctionStore): IO[CoreBlockingAPI] =
    DBInitializer(
      config = config,
      maxSegmentsOpen = maxOpenSegments,
      cacheSize = cacheSize,
      keyValueQueueDelay = cacheCheckDelay,
      segmentCloserDelay = segmentsOpenCheckDelay
    )

  def apply(config: LevelZeroConfig)(implicit ec: ExecutionContext,
                                     keyOrder: KeyOrder[Slice[Byte]],
                                     timeOrder: TimeOrder[Slice[Byte]],
                                     functionStore: FunctionStore): IO[CoreBlockingAPI] =
    DBInitializer(config = config)
}

private[swaydb] case class CoreBlockingAPI(zero: LevelZero) {

  def put(key: Slice[Byte]): IO[Level0Meter] =
    zero.put(key)

  def put(key: Slice[Byte], value: Slice[Byte]): IO[Level0Meter] =
    zero.put(key, value)

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): IO[Level0Meter] =
    zero.put(key, value)

  def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): IO[Level0Meter] =
    zero.put(key, value, removeAt)

  def put(entry: MapEntry[Slice[Byte], Memory.SegmentResponse]): IO[Level0Meter] =
    zero.put(entry)

  def remove(key: Slice[Byte]): IO[Level0Meter] =
    zero.remove(key)

  def remove(key: Slice[Byte], at: Deadline): IO[Level0Meter] =
    zero.remove(key, at)

  def remove(from: Slice[Byte], to: Slice[Byte]): IO[Level0Meter] =
    zero.remove(from, to)

  def remove(from: Slice[Byte], to: Slice[Byte], at: Deadline): IO[Level0Meter] =
    zero.remove(from, to, at)

  def update(key: Slice[Byte], value: Slice[Byte]): IO[Level0Meter] =
    zero.update(key, value)

  def update(key: Slice[Byte], value: Option[Slice[Byte]]): IO[Level0Meter] =
    zero.update(key, value)

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): IO[Level0Meter] =
    zero.update(fromKey, to, value)

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): IO[Level0Meter] =
    zero.update(fromKey, to, value)

  def function(key: Slice[Byte], function: Slice[Byte]): IO[Level0Meter] =
    zero.function(key, function)

  def function(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): IO[Level0Meter] =
    zero.function(from, to, function)

  def head: IO[Option[KeyValueTuple]] =
    zero.head.safeGetBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafe(response.getOrFetchValue.get).safeGetBlocking map {
              result =>
                Some(response.key, result)
            }
        } getOrElse IO.none
    }

  def headKey: IO[Option[Slice[Byte]]] =
    zero.headKey.safeGetBlocking

  def last: IO[Option[KeyValueTuple]] =
    zero.last.safeGetBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafe(response.getOrFetchValue.get).safeGetBlocking map {
              result =>
                Some(response.key, result)
            }
        } getOrElse IO.none
    }

  def lastKey: IO[Option[Slice[Byte]]] =
    zero.lastKey.safeGetBlocking

  def bloomFilterKeyValueCount: IO[Int] =
    zero.bloomFilterKeyValueCount

  def deadline(key: Slice[Byte]): IO[Option[Deadline]] =
    zero.deadline(key).safeGetBlocking

  def sizeOfSegments: Long =
    zero.sizeOfSegments

  def contains(key: Slice[Byte]): IO[Boolean] =
    zero.contains(key).safeGetBlocking

  def mightContain(key: Slice[Byte]): IO[Boolean] =
    zero.mightContain(key)

  def get(key: Slice[Byte]): IO[Option[Option[Slice[Byte]]]] =
    zero.get(key).safeGetBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafe(response.getOrFetchValue.get).safeGetBlocking map {
              result =>
                Some(result)
            }
        } getOrElse IO.none
    }

  def getKey(key: Slice[Byte]): IO[Option[Slice[Byte]]] =
    zero.getKey(key).safeGetBlocking

  def getKeyValue(key: Slice[Byte]): IO[Option[KeyValueTuple]] =
    zero.get(key).safeGetBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafe(response.getOrFetchValue.get).safeGetBlocking map {
              result =>
                Some(response.key, result)
            }
        } getOrElse IO.none
    }

  def before(key: Slice[Byte]): IO[Option[KeyValueTuple]] =
    zero.lower(key).safeGetBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafe(response.getOrFetchValue.get).safeGetBlocking map {
              result =>
                Some(response.key, result)
            }
        } getOrElse IO.none
    }

  def beforeKey(key: Slice[Byte]): IO[Option[Slice[Byte]]] =
    zero.lower(key).safeGetBlocking.map(_.map(_.key))

  def after(key: Slice[Byte]): IO[Option[KeyValueTuple]] =
    zero.higher(key).safeGetBlocking flatMap {
      result =>
        result map {
          response =>
            IO.Async.runSafe(response.getOrFetchValue.get).safeGetBlocking map {
              result =>
                Some(response.key, result)
            }
        } getOrElse IO.none
    }

  def afterKey(key: Slice[Byte]): IO[Option[Slice[Byte]]] =
    zero.higher(key).safeGetBlocking.map(_.map(_.key))

  def valueSize(key: Slice[Byte]): IO[Option[Int]] =
    zero.valueSize(key).safeGetBlocking

  def level0Meter: Level0Meter =
    zero.level0Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    zero.levelMeter(levelNumber)
}
