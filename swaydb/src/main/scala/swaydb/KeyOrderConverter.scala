/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb

import swaydb.core.util.Eithers
import swaydb.data.order.KeyOrder
import swaydb.slice.Slice
import swaydb.serializers.Serializer

protected object KeyOrderConverter {

  def typedToBytesNullCheck[K](byteKeyOrder: KeyOrder[Slice[Byte]], typedKeyOrder: KeyOrder[K])(implicit serializer: Serializer[K]): KeyOrder[Slice[Byte]] =
    typedToBytes(
      Eithers.nullCheck(
        left = byteKeyOrder,
        right = typedKeyOrder,
        default = KeyOrder.default
      )
    )

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

          private[swaydb] override def comparableKey(key: Slice[Byte]): Slice[Byte] = {
            val typedData = serializer.read(key)
            val comparableKeyTyped = typedKeyOrder.comparableKey(typedData)
            serializer.write(comparableKeyTyped)
          }
        }
    }
}
