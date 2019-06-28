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

package swaydb.core.segment.format.a.entry.reader.value

import swaydb.core.io.reader.{BlockReader, Reader}
import swaydb.core.segment.format.a.block.Values
import swaydb.data.IO
import swaydb.data.slice.Slice

object LazyValueReader {

  val empty =
    new LazyValueReader {
      override val valueReader: BlockReader[Values] =
        BlockReader(Reader.empty, Values.empty)
      override val valueLength: Int = 0
      override val valueOffset: Int = 0
    }

  def apply(reader: BlockReader[Values],
            offset: Int,
            length: Int): LazyValueReader =
    new LazyValueReader {
      override val valueReader: BlockReader[Values] = reader

      override def valueLength: Int = length

      override def valueOffset: Int = offset
    }
}

trait LazyValueReader {

  @volatile var valueOption: Option[Slice[Byte]] = _

  def valueReader: BlockReader[Values]

  def valueLength: Int

  def valueOffset: Int

  //tries fetching the value from the given reader
  private def fetchValue(reader: BlockReader[Values]): IO[Option[Slice[Byte]]] =
    if (valueOption == null)
      Values.read(
        fromOffset = valueOffset,
        length = valueLength,
        reader = reader
      ) map {
        value =>
          valueOption = value
          value
      }
    else
      IO.Success(valueOption)

  def getOrFetchValue: IO[Option[Slice[Byte]]] =
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
