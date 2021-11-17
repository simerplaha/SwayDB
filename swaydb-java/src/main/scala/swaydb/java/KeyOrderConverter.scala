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

package swaydb.java

import swaydb.serializers.Serializer
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder

protected object KeyOrderConverter {

  def toScalaKeyOrder[K](comparatorEither: Either[KeyComparator[Slice[java.lang.Byte]], KeyComparator[K]],
                         keySerializer: Serializer[K]) =
    comparatorEither match {
      case Right(comparator) =>
        new KeyOrder[Slice[Byte]] {
          override def compare(left: Slice[Byte], right: Slice[Byte]): Int = {
            val leftKey = keySerializer.read(left)
            val rightKey = keySerializer.read(right)
            comparator.compare(leftKey, rightKey)
          }
        }

      case Left(comparator) =>
        new KeyOrder[Slice[Byte]] {
          override def compare(x: Slice[Byte], y: Slice[Byte]): Int =
            comparator.compare(x.cast, y.cast)
        }
    }
}
