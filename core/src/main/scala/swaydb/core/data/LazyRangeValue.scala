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

package swaydb.core.data

import swaydb.core.map.serializer.RangeValueSerializer

import scala.util.{Success, Try}

//GAH! Inheritance Yuk! Need to update this code.
trait LazyRangeValue extends LazyValue {

  val id: Int

  @volatile private var fromValue: Option[Value] = null
  @volatile private var rangeValue: Value = null

  def fetchRangeValue: Try[Value] =
    if (rangeValue == null)
      fetchFromAndRangeValue.map(_._2)
    else
      Success(rangeValue)

  def fetchFromValue: Try[Option[Value]] =
    if (fromValue == null)
      fetchFromAndRangeValue.map(_._1)
    else
      Success(fromValue)

  def fetchFromAndRangeValue: Try[(Option[Value], Value)] =
    if (fromValue == null || rangeValue == null)
      getOrFetchValue flatMap {
        case Some(rangeValue) =>
          RangeValueSerializer.read(id, rangeValue) map {
            case fromValueRangeValue @ (fromValue, rangeValue) =>
              this.fromValue = fromValue.map(_.unslice)
              this.rangeValue = rangeValue.unslice
              fromValueRangeValue
          }
        case None =>
          RangeValueSerializer.readRemoveRangeOnly(id) map {
            case fromValueRangeValue @ (fromValue, rangeValue) =>
              this.fromValue = fromValue.map(_.unslice)
              this.rangeValue = rangeValue.unslice
              fromValueRangeValue
          }
      }
    else
      Success(fromValue, rangeValue)

}
