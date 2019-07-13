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

package swaydb.core.io.reader

import swaydb.core.io.file.DBFile
import swaydb.core.segment.format.a.block.Block
import swaydb.data.slice.{Reader, Slice, SliceReaderSafe}

private[swaydb] object Reader {

  val empty = Reader(Slice.emptyBytes)

  def apply(file: DBFile): FileReader =
    new FileReader(file)

  def apply(slice: Slice[Byte]): SliceReaderSafe =
    SliceReaderSafe(slice)

  def apply[B <: Block](reader: Reader, block: B): BlockReader[B] =
    BlockReader[B](
      reader = reader,
      block = block
    )
}
