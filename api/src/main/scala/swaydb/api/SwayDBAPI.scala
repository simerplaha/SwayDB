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

package swaydb.api

import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice

import scala.util.Try

/**
  * For internal use only.
  *
  * Untyped raw bytes API for the core module.
  *
  * If access to this untyped API is required then [[swaydb.serializers.Default.SliceSerializer]]
  * should be used instead.
  */
private[swaydb] trait SwayDBAPI {

  def put(key: Slice[Byte]): Try[Level0Meter]

  def put(key: Slice[Byte], value: Slice[Byte]): Try[Level0Meter]

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): Try[Level0Meter]

  def put(entry: Iterable[swaydb.data.request.Batch]): Try[Level0Meter]

  def remove(key: Slice[Byte]): Try[Level0Meter]

  def head: Try[Option[(Slice[Byte], Option[Slice[Byte]])]]

  def headKey: Try[Option[Slice[Byte]]]

  def last: Try[Option[(Slice[Byte], Option[Slice[Byte]])]]

  def lastKey: Try[Option[Slice[Byte]]]

  def sizeOfSegments: Long

  def keyValueCount: Try[Int]

  def contains(key: Slice[Byte]): Try[Boolean]

  def mightContain(key: Slice[Byte]): Try[Boolean]

  def get(key: Slice[Byte]): Try[Option[Option[Slice[Byte]]]]

  def getKey(key: Slice[Byte]): Try[Option[Slice[Byte]]]

  def getKeyValue(key: Slice[Byte]): Try[Option[(Slice[Byte], Option[Slice[Byte]])]]

  def beforeKey(key: Slice[Byte]): Try[Option[Slice[Byte]]]

  def before(key: Slice[Byte]): Try[Option[(Slice[Byte], Option[Slice[Byte]])]]

  def afterKey(key: Slice[Byte]): Try[Option[Slice[Byte]]]

  def after(key: Slice[Byte]): Try[Option[(Slice[Byte], Option[Slice[Byte]])]]

  def level0Meter: Level0Meter

  def level1Meter: LevelMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter]
}
