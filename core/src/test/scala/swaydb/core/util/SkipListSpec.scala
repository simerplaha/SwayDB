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

package swaydb.core.util

import org.scalatest.{Matchers, WordSpec}
import swaydb.data.order.KeyOrder

class MinMaxSkipListSpec extends SkipListSpec {
  override def isMinMax: Boolean = true
}

sealed trait SkipListSpec extends WordSpec with Matchers {

  def isMinMax: Boolean

  sealed trait OptionalKey
  object Key {
    final object None extends OptionalKey
    case class Some(key: Int) extends OptionalKey
  }

  sealed trait OptionalValue
  object Value {
    final object None extends OptionalValue
    case class Some(value: Int) extends OptionalValue
  }

  implicit val ordering = KeyOrder(Ordering.Int.on[Key.Some](_.key))

  "concurrentSkipList" should {

    val skipList = SkipList.concurrent[OptionalKey, OptionalValue, Key.Some, Value.Some](Key.None, Value.None)

    "get" in {
      skipList.get(Key.Some(11)) shouldBe Value.None

      skipList.put(Key.Some(11), Value.Some(111))
      skipList.get(Key.Some(11)) shouldBe Value.Some(111)

      skipList.clear()
    }
  }
}
