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

import swaydb.core.data.Persistent.Put
import swaydb.core.segment.format.one.SegmentReader
import swaydb.data.slice.{Reader, Slice}

import scala.util.{Success, Try}

trait LazyValue {
  @volatile var valueOption: Option[Slice[Byte]] = None

  val valueReader: Reader

  def valueLength: Int

  def valueOffset: Int

  def unsliceKey: Unit

  def id: Int =
    Put.id

  //tries fetching the value from the given reader
  private def fetchValue(reader: Reader): Try[Option[Slice[Byte]]] = {
    if (valueLength == 0) //if valueLength is 0, don't have to hit the file. Return None
      Success(None)
    else
      valueOption match {
        case value @ Some(_) =>
          Success(value)

        case None =>
          SegmentReader.getValue(valueOffset, valueLength, reader) map {
            value =>
              valueOption = value
              value
          }
      }
  }

  def getOrFetchValue: Try[Option[Slice[Byte]]] =
    fetchValue(valueReader)

  def isRemove: Boolean = false

  def isValueDefined: Boolean = valueOption.isDefined

  def getValue: Option[Slice[Byte]] = valueOption
}
