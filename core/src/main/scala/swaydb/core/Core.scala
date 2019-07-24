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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core

import swaydb.core.data.KeyValue.KeyValueTuple
import swaydb.core.data.SwayFunction
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.io.Tag
import swaydb.data.slice.Slice
import swaydb.{IO, Prepare}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Deadline

private[swaydb] trait Core[T[_]] {

  def put(key: Slice[Byte]): T[IO.Done]
  def put(key: Slice[Byte], value: Slice[Byte]): T[IO.Done]
  def put(key: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done]
  def put(key: Slice[Byte], value: Option[Slice[Byte]], removeAt: Deadline): T[IO.Done]
  def put(entries: Iterable[Prepare[Slice[Byte], Option[Slice[Byte]]]]): T[IO.Done]

  def remove(key: Slice[Byte]): T[IO.Done]
  def remove(key: Slice[Byte], at: Deadline): T[IO.Done]
  def remove(from: Slice[Byte], to: Slice[Byte]): T[IO.Done]
  def remove(from: Slice[Byte], to: Slice[Byte], at: Deadline): T[IO.Done]

  def update(key: Slice[Byte], value: Slice[Byte]): T[IO.Done]
  def update(key: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done]
  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Slice[Byte]): T[IO.Done]
  def update(fromKey: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]]): T[IO.Done]

  def function(key: Slice[Byte], function: Slice[Byte]): T[IO.Done]
  def function(from: Slice[Byte], to: Slice[Byte], function: Slice[Byte]): T[IO.Done]
  def registerFunction(functionID: Slice[Byte], function: SwayFunction): SwayFunction

  def head: T[Option[KeyValueTuple]]
  def headKey: T[Option[Slice[Byte]]]

  def last: T[Option[KeyValueTuple]]
  def lastKey: T[Option[Slice[Byte]]]

  def contains(key: Slice[Byte]): T[Boolean]
  def mightContainKey(key: Slice[Byte]): T[Boolean]
  def mightContainFunction(functionId: Slice[Byte]): T[Boolean]

  def get(key: Slice[Byte]): T[Option[Option[Slice[Byte]]]]
  def getKey(key: Slice[Byte]): T[Option[Slice[Byte]]]
  def getKeyValue(key: Slice[Byte]): T[Option[KeyValueTuple]]

  def before(key: Slice[Byte]): T[Option[KeyValueTuple]]
  def beforeKey(key: Slice[Byte]): T[Option[Slice[Byte]]]

  def after(key: Slice[Byte]): T[Option[KeyValueTuple]]
  def afterKey(key: Slice[Byte]): T[Option[Slice[Byte]]]

  def valueSize(key: Slice[Byte]): T[Option[Int]]

  def level0Meter: LevelZeroMeter
  def levelMeter(levelNumber: Int): Option[LevelMeter]

  def tagAsync[T[_]](implicit ec: ExecutionContext, tag: Tag.Async[T]): Core[T]
  def tagBlocking[T[_]](implicit tag: Tag[T]): BlockingCore[T]

  def close(): T[Unit]
  def delete(): T[Unit]

  def bloomFilterKeyValueCount: T[Int]
  def sizeOfSegments: Long

  def deadline(key: Slice[Byte]): T[Option[Deadline]]

  def clear(): T[IO.Done]
}
