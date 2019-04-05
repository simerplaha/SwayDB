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
import scala.concurrent.duration.Deadline
import swaydb.Prepare
import swaydb.core.data.KeyValue.KeyValueTuple
import swaydb.core.data.SwayFunction
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.io.{AsyncIOTransformer, BlockingIOTransformer}
import swaydb.data.io.AsyncIOTransformer
import swaydb.data.slice.Slice

private[swaydb] trait Core[W[_]] {

  def put(key: Slice[Byte]): W[Level0Meter]

  def put(key: Slice[Byte], value: Slice[Byte]): W[Level0Meter]

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): W[Level0Meter]

  def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): W[Level0Meter]

  def put(entries: Iterable[Prepare[Slice[Byte], Option[Slice[Byte]]]]): W[Level0Meter]

  def remove(key: Slice[Byte]): W[Level0Meter]

  def remove(key: Slice[Byte], at: Deadline): W[Level0Meter]

  def remove(from: Slice[Byte], to: Slice[Byte]): W[Level0Meter]

  def remove(from: Slice[Byte], to: Slice[Byte], at: Deadline): W[Level0Meter]

  def update(key: Slice[Byte], value: Slice[Byte]): W[Level0Meter]

  def update(key: Slice[Byte], value: Option[Slice[Byte]]): W[Level0Meter]

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): W[Level0Meter]

  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): W[Level0Meter]

  def clear(): W[Level0Meter]

  def function(key: Slice[Byte], function: Slice[Byte]): W[Level0Meter]

  def function(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): W[Level0Meter]

  def registerFunction(functionID: Slice[Byte], function: SwayFunction): SwayFunction

  def head: W[Option[KeyValueTuple]]

  def headKey: W[Option[Slice[Byte]]]

  def last: W[Option[KeyValueTuple]]

  def lastKey: W[Option[Slice[Byte]]]

  def bloomFilterKeyValueCount: W[Int]

  def deadline(key: Slice[Byte]): W[Option[Deadline]]

  def sizeOfSegments: Long

  def contains(key: Slice[Byte]): W[Boolean]

  def mightContain(key: Slice[Byte]): W[Boolean]

  def get(key: Slice[Byte]): W[Option[Option[Slice[Byte]]]]

  def getKey(key: Slice[Byte]): W[Option[Slice[Byte]]]

  def getKeyValue(key: Slice[Byte]): W[Option[KeyValueTuple]]

  def before(key: Slice[Byte]): W[Option[KeyValueTuple]]

  def beforeKey(key: Slice[Byte]): W[Option[Slice[Byte]]]

  def after(key: Slice[Byte]): W[Option[KeyValueTuple]]

  def afterKey(key: Slice[Byte]): W[Option[Slice[Byte]]]

  def valueSize(key: Slice[Byte]): W[Option[Int]]

  def level0Meter: Level0Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter]

  def close(): W[Unit]

  def async[T[_]](implicit ec: ExecutionContext, converter: AsyncIOTransformer[T]): Core[T]

  def blocking[T[_]](implicit converter: BlockingIOTransformer[T]): BlockingCore[T]
}
