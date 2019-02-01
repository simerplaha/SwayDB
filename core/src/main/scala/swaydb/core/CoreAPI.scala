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

import swaydb.core.data.KeyValue._
import swaydb.core.data.Memory
import swaydb.core.function.FunctionStore
import swaydb.core.level.zero.LevelZero
import swaydb.core.map.MapEntry
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.config.SwayDBConfig
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Deadline, FiniteDuration}
import swaydb.data.io.IO

private[swaydb] object CoreAPI {

  def apply(config: SwayDBConfig,
            maxOpenSegments: Int,
            cacheSize: Long,
            cacheCheckDelay: FiniteDuration,
            segmentsOpenCheckDelay: FiniteDuration)(implicit ec: ExecutionContext,
                                                    keyOrder: KeyOrder[Slice[Byte]],
                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                    functionStore: FunctionStore): IO[CoreAPI] =
    DBInitializer(
      config = config,
      maxSegmentsOpen = maxOpenSegments,
      cacheSize = cacheSize,
      keyValueQueueDelay = cacheCheckDelay,
      segmentCloserDelay = segmentsOpenCheckDelay
    )
}

private[swaydb] case class CoreAPI(zero: LevelZero) {

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

  def remove(fromKey: Slice[Byte], to: Slice[Byte]): IO[Level0Meter] =
    zero.remove(fromKey, to)

  def remove(fromKey: Slice[Byte], to: Slice[Byte], at: Deadline): IO[Level0Meter] =
    zero.remove(fromKey, to, at)

  def update(key: Slice[Byte], value: Slice[Byte]): IO[Level0Meter] =
    zero.update(key, value)

  def update(key: Slice[Byte], value: Option[Slice[Byte]]): IO[Level0Meter] =
    zero.update(key, value)

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): IO[Level0Meter] =
    zero.update(fromKey, to, value)

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): IO[Level0Meter] =
    zero.update(fromKey, to, value)

  def head: IO[Option[KeyValueTuple]] =
    LevelZero.withRetry("CoreAPI.head", zero.readRetryLimit) {
      zero.head flatMap {
        result =>
          result map {
            response =>
              response.getOrFetchValue map {
                result =>
                  Some(response.key, result)
              }
          } getOrElse IO.successNone
      }
    }

  def headKey: IO[Option[Slice[Byte]]] =
    zero.headKey

  def last: IO[Option[KeyValueTuple]] =
    LevelZero.withRetry("CoreAPI.last", zero.readRetryLimit) {
      zero.last flatMap {
        result =>
          result map {
            response =>
              response.getOrFetchValue map {
                result =>
                  Some(response.key, result)
              }
          } getOrElse IO.successNone
      }
    }

  def lastKey: IO[Option[Slice[Byte]]] =
    zero.lastKey

  def bloomFilterKeyValueCount: IO[Int] =
    zero.bloomFilterKeyValueCount

  def deadline(key: Slice[Byte]): IO[Option[Deadline]] =
    zero.deadline(key)

  def sizeOfSegments: Long =
    zero.sizeOfSegments

  def contains(key: Slice[Byte]): IO[Boolean] =
    zero.contains(key)

  def mightContain(key: Slice[Byte]): IO[Boolean] =
    zero.mightContain(key)

  def get(key: Slice[Byte]): IO[Option[Option[Slice[Byte]]]] =
    LevelZero.withRetry("CoreAPI.get", zero.readRetryLimit) {
      zero.get(key) flatMap {
        result =>
          result map {
            response =>
              response.getOrFetchValue map {
                result =>
                  Some(result)
              }
          } getOrElse IO.successNone
      }
    }

  def getKey(key: Slice[Byte]): IO[Option[Slice[Byte]]] =
    zero.getKey(key)

  def getKeyValue(key: Slice[Byte]): IO[Option[KeyValueTuple]] =
    LevelZero.withRetry("CoreAPI.getKeyValue", zero.readRetryLimit) {
      zero.get(key) flatMap {
        result =>
          result map {
            response =>
              response.getOrFetchValue map {
                result =>
                  Some(response.key, result)
              }
          } getOrElse IO.successNone
      }
    }

  def before(key: Slice[Byte]): IO[Option[KeyValueTuple]] =
    LevelZero.withRetry("CoreAPI.before", zero.readRetryLimit) {
      zero.lower(key) flatMap {
        result =>
          result map {
            response =>
              response.getOrFetchValue map {
                result =>
                  Some(response.key, result)
              }
          } getOrElse IO.successNone
      }
    }

  def beforeKey(key: Slice[Byte]): IO[Option[Slice[Byte]]] =
    zero.lower(key).map(_.map(_.key))

  def after(key: Slice[Byte]): IO[Option[KeyValueTuple]] =
    LevelZero.withRetry("CoreAPI.after", zero.readRetryLimit) {
      zero.higher(key) flatMap {
        result =>
          result map {
            response =>
              response.getOrFetchValue map {
                result =>
                  Some(response.key, result)
              }
          } getOrElse IO.successNone
      }
    }

  def afterKey(key: Slice[Byte]): IO[Option[Slice[Byte]]] =
    zero.higher(key).map(_.map(_.key))

  def valueSize(key: Slice[Byte]): IO[Option[Int]] =
    zero.valueSize(key)

  def level0Meter: Level0Meter =
    zero.level0Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    zero.levelMeter(levelNumber)
}
