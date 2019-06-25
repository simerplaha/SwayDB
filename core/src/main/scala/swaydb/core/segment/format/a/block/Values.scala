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

package swaydb.core.segment.format.a.block

import swaydb.compression.CompressionInternal
import swaydb.core.data.KeyValue
import swaydb.core.segment.format.a.OffsetBase
import swaydb.data.slice.Slice

object Values {

  val uncompressedFormatId = 1.toByte

  case class State(bytes: Slice[Byte],
                   compressions: Seq[CompressionInternal])

  case class Offset(start: Int, size: Int) extends OffsetBase

  def init(keyValues: Iterable[KeyValue.WriteOnly],
           compressions: Seq[CompressionInternal]): Option[Values.State] =
    if (keyValues.last.stats.segmentValuesSize > 0)
      Some(
        Values.State(
          bytes = Slice.create[Byte](keyValues.last.stats.segmentValuesSize),
          compressions = compressions
        )
      )
    else
      None

  def write(value: Slice[Byte], state: State) =
    state.bytes addAll value

  def close(state: State) =
    ???
}
