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

package swaydb.core.segment.format.a.entry.reader.value

import scala.util.{Success, Try}
import swaydb.core.segment.format.a.SegmentReader
import swaydb.data.slice.{Reader, Slice}

object LazyValueReader {
  def apply(reader: Reader,
            offset: Int,
            length: Int): LazyValueReader =
    new LazyValueReader {
      override val valueReader: Reader = reader

      override def valueLength: Int = length

      override def valueOffset: Int = offset
    }
}

trait LazyValueReader {

  @volatile var valueOption: Option[Slice[Byte]] = _

  def valueReader: Reader

  def valueLength: Int

  def valueOffset: Int

  //tries fetching the value from the given reader
  private def fetchValue(reader: Reader): Try[Option[Slice[Byte]]] =
    if (valueOption == null)
      SegmentReader.readBytes(valueOffset, valueLength, reader) map {
        value =>
          valueOption = value
          value
      }
    else
      Success(valueOption)

  def getOrFetchValue: Try[Option[Slice[Byte]]] =
    fetchValue(valueReader)

  def isValueDefined: Boolean =
    valueOption != null

  def getValue: Option[Slice[Byte]] =
    valueOption

  override def equals(that: Any): Boolean =
    that match {
      case other: LazyValueReader =>
        getOrFetchValue == other.getOrFetchValue

      case _ =>
        false
    }

}
