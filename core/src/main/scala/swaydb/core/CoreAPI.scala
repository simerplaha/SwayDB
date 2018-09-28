/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
import swaydb.core.map.MapEntry
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.config.SwayDBConfig
import swaydb.data.slice.Slice

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.Try

private[swaydb] object CoreAPI {

  def apply(config: SwayDBConfig,
            maxOpenSegments: Int,
            cacheSize: Long,
            cacheCheckDelay: FiniteDuration,
            segmentsOpenCheckDelay: FiniteDuration)(implicit ec: ExecutionContext,
                                                           ordering: Ordering[Slice[Byte]]): Try[CoreAPI] =
    DBInitializer(
      config = config,
      maxSegmentsOpen = maxOpenSegments,
      cacheSize = cacheSize,
      keyValueQueueDelay = cacheCheckDelay,
      segmentCloserDelay = segmentsOpenCheckDelay
    )
}

private[swaydb] trait CoreAPI {

  def put(key: Slice[Byte]): Try[Level0Meter]

  def put(key: Slice[Byte], value: Slice[Byte]): Try[Level0Meter]

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): Try[Level0Meter]

  def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): Try[Level0Meter]

  def put(entry: MapEntry[Slice[Byte], Memory.Response]): Try[Level0Meter]

  def remove(key: Slice[Byte]): Try[Level0Meter]

  def remove(key: Slice[Byte], at: Deadline): Try[Level0Meter]

  def remove(fromKey: Slice[Byte], to: Slice[Byte]): Try[Level0Meter]

  def remove(fromKey: Slice[Byte], to: Slice[Byte], at: Deadline): Try[Level0Meter]

  def update(key: Slice[Byte], value: Slice[Byte]): Try[Level0Meter]

  def update(key: Slice[Byte], value: Option[Slice[Byte]]): Try[Level0Meter]

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): Try[Level0Meter]

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): Try[Level0Meter]

  def head: Try[Option[KeyValueTuple]]

  def headKey: Try[Option[Slice[Byte]]]

  def last: Try[Option[KeyValueTuple]]

  def lastKey: Try[Option[Slice[Byte]]]

  def keyValueCount: Try[Int]

  def deadline(key: Slice[Byte]): Try[Option[Deadline]]

  def sizeOfSegments: Long

  def contains(key: Slice[Byte]): Try[Boolean]

  def mightContain(key: Slice[Byte]): Try[Boolean]

  def get(key: Slice[Byte]): Try[Option[Option[Slice[Byte]]]]

  def getKey(key: Slice[Byte]): Try[Option[Slice[Byte]]]

  def getKeyValue(key: Slice[Byte]): Try[Option[KeyValueTuple]]

  def valueSize(key: Slice[Byte]): Try[Option[Int]]

  def beforeKey(key: Slice[Byte]): Try[Option[Slice[Byte]]]

  def before(key: Slice[Byte]): Try[Option[KeyValueTuple]]

  def afterKey(key: Slice[Byte]): Try[Option[Slice[Byte]]]

  def after(key: Slice[Byte]): Try[Option[KeyValueTuple]]

  def level0Meter: Level0Meter

  def level1Meter: LevelMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter]
}