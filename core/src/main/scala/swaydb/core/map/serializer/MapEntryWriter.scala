/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.map.serializer

import swaydb.core.map.MapEntry
import swaydb.data.slice.Slice
import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for MapEntryWriter of type ${T}")
trait MapEntryWriter[T <: MapEntry[_, _]] {
  def write(entry: T, bytes: Slice[Byte]): Unit

  def bytesRequired(entry: T): Int

  val isRange: Boolean

  val isUpdate: Boolean
}

object MapEntryWriter {

  def write[T <: MapEntry[_, _]](entry: T, bytes: Slice[Byte])(implicit serializer: MapEntryWriter[T]): Unit =
    serializer.write(entry, bytes)

  def bytesRequired[T <: MapEntry[_, _]](entry: T)(implicit serializer: MapEntryWriter[T]): Int =
    serializer.bytesRequired(entry)
}
