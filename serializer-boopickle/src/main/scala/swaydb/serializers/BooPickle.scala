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

package swaydb.serializers

import boopickle.Default.{Pickle, Unpickle}
import boopickle.{PickleState, Pickler}
import swaydb.data.slice.Slice

object BooPickle {

  def apply[A](implicit picker: Pickler[A]): Serializer[A] =
    new swaydb.serializers.Serializer[A] {
      override def write(data: A): Slice[Byte] = {
        implicit val state: PickleState = PickleState.pickleStateSpeed
        val buffer = Pickle.intoBytes(data)
        Slice.ofScala(buffer, 0, buffer.limit() - 1)
      }

      override def read(slice: Slice[Byte]): A =
        Unpickle[A].fromBytes(slice.toByteBufferWrap)
    }
}
