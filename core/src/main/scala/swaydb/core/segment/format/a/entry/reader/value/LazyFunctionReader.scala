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

import swaydb.core.io.reader.BlockReader
import swaydb.core.segment.format.a.block.Values
import swaydb.data.IO
import swaydb.data.slice.Slice

private[core] object LazyFunctionReader {
  def apply(reader: BlockReader[Values],
            offset: Int,
            length: Int): LazyFunctionReader =
    new LazyFunctionReader {
      override val valueReader: BlockReader[Values] = reader

      override def valueLength: Int = length

      override def valueOffset: Int = offset
    }
}

private[core] trait LazyFunctionReader extends LazyValueReader {

  def getOrFetchFunction: IO[Slice[Byte]] =
    super.getOrFetchValue flatMap {
      case Some(value) =>
        IO.Success(value)
      case None =>
        IO.Failure(new Exception("Empty functionId."))
    }

  override def isValueDefined: Boolean =
    super.isValueDefined
}
