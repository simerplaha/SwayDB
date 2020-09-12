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

package swaydb.core.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.util.skiplist.{SkipListConcurrent, SkipList}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.slice.Slice

import swaydb.serializers._
import swaydb.serializers.Default._

class SkipListConcurrentSpec extends SkipListSpec
sealed trait SkipListSpec extends AnyWordSpec with Matchers {

  sealed trait ValueOption
  object Value {
    final case object Null extends ValueOption
    case class Some(value: Int) extends ValueOption
  }

  implicit val ordering = KeyOrder.default

  def createSkipList(): SkipListConcurrent[SliceOption[Byte], ValueOption, Slice[Byte], Value.Some] =
    SkipList.concurrent[SliceOption[Byte], ValueOption, Slice[Byte], Value.Some](Slice.Null, Value.Null)

  "put & putIfAbsent" in {
    val skipList = createSkipList()
    skipList.put(1, Value.Some(1))
    skipList.get(1) shouldBe Value.Some(1)

    skipList.putIfAbsent(1, Value.Some(123)) shouldBe false
    skipList.get(1) shouldBe Value.Some(1)

    skipList.putIfAbsent(2, Value.Some(22)) shouldBe true
    skipList.get(2) shouldBe Value.Some(22)
  }

  "get" in {
    val skipList = createSkipList()
    skipList.get(1) shouldBe Value.Null

    skipList.put(11, Value.Some(111))
    skipList.get(11) shouldBe Value.Some(111)
  }

  "remove" in {
    val skipList = createSkipList()
    skipList.put(11, Value.Some(111))
    skipList.get(11) shouldBe Value.Some(111)

    skipList.isEmpty shouldBe false
    skipList.remove(11)
    skipList.isEmpty shouldBe true

    skipList.get(11) shouldBe Value.Null
  }

  "lower & lowerKey" in {
    val skipList = createSkipList()
    skipList.put(1, Value.Some(1))

    skipList.lower(2) shouldBe Value.Some(1)
    skipList.lower(1) shouldBe Value.Null
    skipList.lower(0) shouldBe Value.Null

    skipList.lowerKey(2) shouldBe (1: Slice[Byte])
    skipList.lowerKey(1) shouldBe Slice.Null
    skipList.lowerKey(0) shouldBe Slice.Null
  }

  "floor" in {
    val skipList = createSkipList()
    skipList.put(1, Value.Some(1))

    skipList.floor(1) shouldBe Value.Some(1)
    skipList.floor(0) shouldBe Value.Null
  }

  "higher" in {
    val skipList = createSkipList()
    skipList.put(1, Value.Some(1))

    skipList.higher(0) shouldBe Value.Some(1)
    skipList.higher(1) shouldBe Value.Null
  }

  "ceiling & ceilingKey" in {
    val skipList = createSkipList()
    skipList.put(1, Value.Some(1))

    skipList.ceiling(0) shouldBe Value.Some(1)
    skipList.ceiling(1) shouldBe Value.Some(1)
    skipList.ceiling(2) shouldBe Value.Null

    skipList.ceilingKey(0) shouldBe (1: Slice[Byte])
    skipList.ceilingKey(1) shouldBe (1: Slice[Byte])
    skipList.ceilingKey(2) shouldBe Slice.Null
  }

  "head, last, headKey & lastKey" in {
    val skipList = createSkipList()
    skipList.put(1, Value.Some(1))
    skipList.put(2, Value.Some(2))

    skipList.head() shouldBe Value.Some(1)
    skipList.last() shouldBe Value.Some(2)
    skipList.headKey shouldBe (1: Slice[Byte])
    skipList.lastKey shouldBe (2: Slice[Byte])

    skipList.isEmpty shouldBe false
    skipList.clear()
    skipList.isEmpty shouldBe true

    skipList.head() shouldBe Value.Null
    skipList.last() shouldBe Value.Null
    skipList.headKey shouldBe Slice.Null
    skipList.lastKey shouldBe Slice.Null
  }
}
