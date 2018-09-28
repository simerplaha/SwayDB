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

package swaydb.core.data.`lazy`

import swaydb.core.segment.format.one.SegmentReader
import swaydb.data.slice.{Reader, Slice}

import scala.util.{Failure, Success, Try}

trait LazyValue {

  @volatile var valueOption: Option[Slice[Byte]] = _

  val valueReader: Reader

  def valueLength: Int

  def valueOffset: Int

  //tries fetching the value from the given reader
  private def fetchValue(reader: Reader): Try[Option[Slice[Byte]]] =
    if (valueOption == null)
      SegmentReader.readBytes(valueOffset, valueLength, reader) map {
        value =>
          valueOption = value
          value
      } recoverWith {
        case ex =>
          Failure(ex)
      }
    else
      Success(valueOption)

  def getOrFetchValue: Try[Option[Slice[Byte]]] =
    fetchValue(valueReader)

  def isValueDefined: Boolean =
    valueOption != null

  def getValue: Option[Slice[Byte]] =
    valueOption
}
