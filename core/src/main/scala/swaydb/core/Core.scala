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

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Deadline
import swaydb.Prepare
import swaydb.core.data.KeyValue.KeyValueTuple
import swaydb.core.data.SwayFunction
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice

private[swaydb] trait Core[F[_]] {

  def put(key: Slice[Byte]): F[Level0Meter]

  def put(key: Slice[Byte], value: Slice[Byte]): F[Level0Meter]

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): F[Level0Meter]

  def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): F[Level0Meter]

  def put(entries: Iterable[Prepare[Slice[Byte], Option[Slice[Byte]]]]): F[Level0Meter]

  def remove(key: Slice[Byte]): F[Level0Meter]

  def remove(key: Slice[Byte], at: Deadline): F[Level0Meter]

  def remove(from: Slice[Byte], to: Slice[Byte]): F[Level0Meter]

  def remove(from: Slice[Byte], to: Slice[Byte], at: Deadline): F[Level0Meter]

  def update(key: Slice[Byte], value: Slice[Byte]): F[Level0Meter]

  def update(key: Slice[Byte], value: Option[Slice[Byte]]): F[Level0Meter]

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): F[Level0Meter]

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): F[Level0Meter]

  def function(key: Slice[Byte], function: Slice[Byte]): F[Level0Meter]

  def function(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): F[Level0Meter]

  def registerFunction(functionID: Slice[Byte], function: SwayFunction): SwayFunction

  def head: F[Option[KeyValueTuple]]

  def headKey: F[Option[Slice[Byte]]]

  def last: F[Option[KeyValueTuple]]

  def lastKey: F[Option[Slice[Byte]]]

  def bloomFilterKeyValueCount: F[Int]

  def deadline(key: Slice[Byte]): F[Option[Deadline]]

  def sizeOfSegments: Long

  def contains(key: Slice[Byte]): F[Boolean]

  def mightContain(key: Slice[Byte]): F[Boolean]

  def get(key: Slice[Byte]): F[Option[Option[Slice[Byte]]]]

  def getKey(key: Slice[Byte]): F[Option[Slice[Byte]]]

  def getKeyValue(key: Slice[Byte]): F[Option[KeyValueTuple]]

  def before(key: Slice[Byte]): F[Option[KeyValueTuple]]

  def beforeKey(key: Slice[Byte]): F[Option[Slice[Byte]]]

  def after(key: Slice[Byte]): F[Option[KeyValueTuple]]

  def afterKey(key: Slice[Byte]): F[Option[Slice[Byte]]]

  def valueSize(key: Slice[Byte]): F[Option[Int]]

  def level0Meter: Level0Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter]

  def close(): F[Unit]

  def async()(implicit ec: ExecutionContext): Core[Future]

}
