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
 */

package swaydb

import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

protected object KeyOrderConverter {

  def typedToBytes[K](keyOrder: Either[KeyOrder[Slice[Byte]], KeyOrder[K]])(implicit serializer: Serializer[K]): KeyOrder[Slice[Byte]] =
    keyOrder match {
      case Left(bytesKeyOrder) =>
        bytesKeyOrder

      case Right(typedKeyOrder) =>
        new KeyOrder[Slice[Byte]] {
          override def compare(key1: Slice[Byte], key2: Slice[Byte]): Int = {
            val typedKey1 = serializer.read(key1)
            val typedKey2 = serializer.read(key2)
            typedKeyOrder.compare(typedKey1, typedKey2)
          }
        }
    }
}
