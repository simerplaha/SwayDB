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

package swaydb.slice

import swaydb.slice.order.KeyOrder
import swaydb.slice.Slice
import swaydb.utils.SomeOrNoneCovariant

sealed trait MaxKeyOption[+T] extends SomeOrNoneCovariant[MaxKeyOption[T], MaxKey[T]] {
  override def noneC: MaxKeyOption[Nothing] = MaxKey.Null
}

sealed trait MaxKey[+T] extends MaxKeyOption[T] {
  def maxKey: T
  def inclusive: Boolean
  override def isNoneC: Boolean = false
  override def getC: MaxKey[T] = this
}

object MaxKey {

  final case object Null extends MaxKeyOption[Nothing] {
    override def isNoneC: Boolean = true
    override def getC: MaxKey[Nothing] = throw new Exception("MaxKey is of type Null")
  }

  def within[T](key: Slice[T],
                minKey: Slice[T],
                maxKey: MaxKey[Slice[T]])(implicit keyOrder: Ordering[Slice[T]]): Boolean =
    Slice.within(
      key = key,
      minKey = minKey,
      maxKey = maxKey.maxKey,
      maxKeyInclusive = maxKey.inclusive
    )


  def within[T](source: MaxKey[T],
                target: MaxKey[T])(implicit keyOrder: Ordering[T]): Boolean = {
    import keyOrder._

    source match {
      case MaxKey.Fixed(sourceMaxKey) =>
        target match {
          case MaxKey.Fixed(targetMaxKey) =>
            keyOrder.equiv(sourceMaxKey, targetMaxKey)

          case MaxKey.Range(targetFromKey, targetMaxKey) =>
            Slice.within(
              key = sourceMaxKey,
              minKey = targetFromKey,
              maxKey = targetMaxKey,
              maxKeyInclusive = false
            )
        }

      case MaxKey.Range(sourceFromKey, sourceMaxKey) =>
        target match {
          case MaxKey.Fixed(targetMaxKey) =>
            keyOrder.equiv(sourceFromKey, targetMaxKey) &&
              keyOrder.equiv(sourceMaxKey, targetMaxKey)

          case MaxKey.Range(targetFromKey, targetMaxKey) =>
            Slice.within(
              key = sourceFromKey,
              minKey = targetFromKey,
              maxKey = targetMaxKey,
              maxKeyInclusive = false
            ) && sourceMaxKey <= targetMaxKey
        }
    }
  }

  implicit class MaxKeyImplicits(maxKey: MaxKey[Slice[Byte]]) {
    @inline final def cut(): MaxKey[Slice[Byte]] =
      maxKey match {
        case Fixed(maxKey) =>
          Fixed(maxKey.cut())

        case Range(fromKey, maxKey) =>
          Range(fromKey.cut(), maxKey.cut())
      }

    @inline final def lessThan(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
      import keyOrder._
      (maxKey.inclusive && maxKey.maxKey < key) || (!maxKey.inclusive && maxKey.maxKey <= key)
    }
  }

  case class Fixed[+T](maxKey: T) extends MaxKey[T] {
    override def inclusive: Boolean = true
  }

  case class Range[+T](fromKey: T, maxKey: T) extends MaxKey[T] {
    override def inclusive: Boolean = false
  }
}
