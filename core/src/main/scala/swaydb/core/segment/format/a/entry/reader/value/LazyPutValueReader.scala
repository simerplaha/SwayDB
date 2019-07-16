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

private[core] object LazyPutValueReader {
  def apply(reader: UnblockedReader[ValuesBlock],
            offset: Int,
            length: Int): LazyPutValueReader =
    new LazyPutValueReader {
      override val valueReader: UnblockedReader[ValuesBlock] = reader

      override def valueLength: Int = length

      override def valueOffset: Int = offset
    }
}

private[core] trait LazyPutValueReader extends LazyValueReader {

  override def getOrFetchValue: IO[Option[Slice[Byte]]] =
    super.getOrFetchValue

  override def isValueDefined: Boolean =
    super.isValueDefined
}
