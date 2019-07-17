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

import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.data.IO
import swaydb.data.slice.Slice

private[core] object LazyValueReader {

  val empty =
    new LazyValueReader {
      override val valueReader: UnblockedReader[ValuesBlock.Offset, ValuesBlock] =
        ValuesBlock.emptyUnblocked

      override val valueLength: Int = 0
      override val valueOffset: Int = 0
    }

  def apply(reader: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
            offset: Int,
            length: Int): LazyValueReader =
    new LazyValueReader {
      override val valueReader: UnblockedReader[ValuesBlock.Offset, ValuesBlock] = reader

      override def valueLength: Int = length

      override def valueOffset: Int = offset
    }
}

private[core] trait LazyValueReader {

  @volatile var valueOption: Option[Slice[Byte]] = _

  def valueReader: UnblockedReader[ValuesBlock.Offset, ValuesBlock]

  def valueLength: Int

  def valueOffset: Int

  //tries fetching the value from the given reader
  private def fetchValue(reader: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): IO[Option[Slice[Byte]]] =
    if (valueOption == null)
      ValuesBlock.read(
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
