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
import swaydb.utils.DropIterator

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
