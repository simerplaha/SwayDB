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

package swaydb.utils

import scala.jdk.CollectionConverters._

object NonEmptyList {

  def apply[F](head: F, tail: F*): NonEmptyList[F] =
    new NonEmptyList[F](head, tail)
}

case class NonEmptyList[F](override val head: F, override val tail: Iterable[F]) extends Iterable[F] { self =>

  override def iterator: Iterator[F] =
    new Iterator[F] {
      var processedHead = false
      val tailIterator = self.tail.iterator

      override def hasNext: Boolean =
        !processedHead || tailIterator.hasNext

      override def next(): F =
        if (!processedHead) {
          processedHead = true
          head
        } else {
          tailIterator.next()
        }
    }

  def asJava: java.util.Iterator[F] =
    this.iterator.asJava
}
