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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.core.data.Value.FromValue
import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class DropIteratorSpec extends AnyWordSpec with Matchers {

  implicit val testTimer: TestTimer = TestTimer.Empty

  implicit def toPut(key: Int): Memory.Put =
    Memory.put(key)

  val range = Memory.Range(1, 2, FromValue.Null, Value.update(1))

  "it" should {
    //mutate the state of this List and assert.
    var list: DropIterator[KeyValue.Range, KeyValue] = DropIterator[KeyValue.Range, KeyValue](Slice[KeyValue](1, 2, 3))

    "store key-values" in {
      list.depth shouldBe 1
    }

    "drop" in {
      list = list.dropHead()
      list.depth shouldBe 1

      val expect = Slice[KeyValue](2, 3)
      list.iterator.toSeq shouldBe expect
      list = DropIterator(expect)
    }

    "dropAppend" in {
      list = list.dropPrepend(range)
      list.depth shouldBe 1

      val expect = Slice[KeyValue](range, 3)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)
    }

    "merge" in {
      list = list append DropIterator(Slice[KeyValue](4, 5, 6))
      list.depth shouldBe 2

      val expect = Slice[KeyValue](range, 3, 4, 5, 6)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)
    }

    "drop range" in {
      list = list.dropHead()
      list.depth shouldBe 1

      val expect = Slice[KeyValue](3, 4, 5, 6)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)
    }

    "drop again & depth should be 1 since all the key-values from first list are removed" in {
      list = list.dropHead()
      list.depth shouldBe 1
      var expect = Slice[KeyValue](4, 5, 6)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)

      //drop again, depth goes down to 1
      list = list.dropHead()
      list.depth shouldBe 1
      expect = Slice[KeyValue](5, 6)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)

      //drop again
      list = list.dropHead()
      list.depth shouldBe 1
      expect = Slice[KeyValue](6)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)

      //drop again
      list = list.dropHead()
      list.depth shouldBe 1
      list.iterator shouldBe empty
    }
  }

  "merging multiple DropLists" should {
    //mutate the state of this List and assert.
    var list =
      DropIterator[KeyValue.Range, KeyValue](Slice[KeyValue](1, 2)) append
        DropIterator[KeyValue.Range, KeyValue](Slice[KeyValue](3, 4)) append
        DropIterator[KeyValue.Range, KeyValue](Slice[KeyValue](5, 6)) append
        DropIterator[KeyValue.Range, KeyValue](Slice[KeyValue](7, 8))

    val range = Memory.Range(1, 2, FromValue.Null, Value.update(1))

    "store key-values" in {
      list.depth shouldBe 4
    }

    "drop" in {
      list = list.dropHead()
      list.depth shouldBe 4
      val expect = Slice[KeyValue](2, 3, 4, 5, 6, 7, 8)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)
    }

    "dropAppend" in {
      list = list.dropPrepend(range)
      list.depth shouldBe 1
      val expect = Slice[KeyValue](range, 3, 4, 5, 6, 7, 8)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)
    }

    "merge" in {
      list = DropIterator[KeyValue.Range, KeyValue](Slice[KeyValue](9)) append list
      list.depth shouldBe 2
      val expect = Slice[KeyValue](9, range, 3, 4, 5, 6, 7, 8)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)
    }

    "drop 2" in {
      list = list.dropHead()
      list.depth shouldBe 1
      var expect = Slice[KeyValue](range, 3, 4, 5, 6, 7, 8)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)

      list = list.dropHead()
      list.depth shouldBe 1
      expect = Slice[KeyValue](3, 4, 5, 6, 7, 8)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)

      list = list.dropHead()
      list = list.dropHead()
      list.depth shouldBe 1
      expect = Slice[KeyValue](5, 6, 7, 8)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)

      list = list.dropHead()
      list = list.dropHead()
      list = list.dropHead()
      list = list.dropHead()
      list.depth shouldBe 1
    }

    "merge when empty" in {
      list = list append list append list append list append DropIterator(Slice[KeyValue](9))
      list.depth shouldBe 1
      var expect = Slice[KeyValue](9)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)

      list.dropHead()
      list.depth shouldBe 1

      list = DropIterator[KeyValue.Range, KeyValue](Slice[KeyValue](1)) append list append list append list append list
      list.depth shouldBe 1
      expect = Slice[KeyValue](1)
      list.iterator.toList shouldBe expect
      list = DropIterator(expect)

      list.dropHead()
      list.depth shouldBe 1
    }
  }

  "dropHeadDuplicate" in {
    val iterator = DropIterator[KeyValue.Range, KeyValue](Slice[KeyValue](10, 11, 12, 13))
    iterator.dropPrepend(range) //[Range, 2, 3, 4]

    val duplicate = iterator.dropHeadDuplicate()
    duplicate.iterator.map(_.key.readInt()).toList shouldBe List(11, 12, 13)

    iterator.headOrNull shouldBe range
    iterator.iterator.map(_.key.readInt()).toList shouldBe List(1, 11, 12, 13)
  }
}
