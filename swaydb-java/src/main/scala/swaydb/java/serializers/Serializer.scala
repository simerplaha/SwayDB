/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.java.serializers

import swaydb.data.slice.Slice

trait Serializer[T] {

  /**
   * You can also use ByteSliceBuilder to build
   * custom serialisation.
   */
  def write(data: T): Slice[java.lang.Byte]

  /**
   * A Slice is a section of Segment's byte array so this
   * Slice could be directly coming from a Segment.
   *
   * Do not mutate this byte array, just read its content build your object.
   */
  def read(slice: Slice[java.lang.Byte]): T

}
