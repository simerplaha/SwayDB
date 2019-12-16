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

package swaydb.core.segment.merge

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class MergeListSpec extends WordSpec with Matchers {

  implicit val testTimer: TestTimer = TestTimer.Empty

  implicit def toPut(key: Int): Memory.Put =
    Memory.put(key)

  "MergeList" should {
    //mutate the state of this List and assert.
    var list = MergeList[KeyValue.Range, KeyValue](Slice[KeyValue](1, 2, 3))
    val range = Memory.Range(1, 2, None, Value.update(1))

    "store key-values" in {
      list.depth shouldBe 1
      list.size shouldBe 3
    }

    "drop" in {
      list = list.dropHead()
      list.depth shouldBe 1
      list.size shouldBe 2
      list.toSlice shouldBe Slice[KeyValue](2, 3)
    }

    "dropAppend" in {
      list = list.dropPrepend(range)
      list.depth shouldBe 1
      list.size shouldBe 2
      list.toSlice shouldBe Slice[KeyValue](range, 3)
    }

    "merge" in {
      list = list append MergeList(Slice[KeyValue](4, 5, 6))
      list.depth shouldBe 2
      list.size shouldBe 5
      list.toSlice shouldBe Slice[KeyValue](range, 3, 4, 5, 6)
    }

    "drop range" in {
      list = list.dropHead()
      list.depth shouldBe 2
      list.size shouldBe 4
      list.toSlice shouldBe Slice[KeyValue](3, 4, 5, 6)
    }

    "drop again & depth should be 1 since all the key-values from first list are removed" in {
      list = list.dropHead()
      list.depth shouldBe 2
      list.size shouldBe 3
      list.toSlice shouldBe Slice[KeyValue](4, 5, 6)

      //drop again, depth goes down to 1
      list = list.dropHead()
      list.depth shouldBe 1
      list.size shouldBe 2
      list.toSlice shouldBe Slice[KeyValue](5, 6)

      //drop again
      list = list.dropHead()
      list.depth shouldBe 1
      list.size shouldBe 1
      list.toSlice shouldBe Slice[KeyValue](6)

      //drop again
      list = list.dropHead()
      list.depth shouldBe 1
      list.size shouldBe 0
      list should have size 0
    }
  }

  "merging multiple MergeLists" should {
    //mutate the state of this List and assert.
    var list =
      MergeList[KeyValue.Range, KeyValue](Slice[KeyValue](1, 2)) append
        MergeList[KeyValue.Range, KeyValue](Slice[KeyValue](3, 4)) append
        MergeList[KeyValue.Range, KeyValue](Slice[KeyValue](5, 6)) append
        MergeList[KeyValue.Range, KeyValue](Slice[KeyValue](7, 8))

    val range = Memory.Range(1, 2, None, Value.update(1))

    "store key-values" in {
      list.depth shouldBe 4
      list.size shouldBe 8
    }

    "drop" in {
      list = list.dropHead()
      list.depth shouldBe 4
      list.size shouldBe 7
      list.toSlice shouldBe Slice[KeyValue](2, 3, 4, 5, 6, 7, 8)
    }

    "dropAppend" in {
      list = list.dropPrepend(range)
      list.depth shouldBe 4
      list.size shouldBe 7
      list.toSlice shouldBe Slice[KeyValue](range, 3, 4, 5, 6, 7, 8)
    }

    "merge" in {
      list = MergeList[KeyValue.Range, KeyValue](Slice[KeyValue](9)) append list
      list.depth shouldBe 5
      list.size shouldBe 8
      list.toSlice shouldBe Slice[KeyValue](9, range, 3, 4, 5, 6, 7, 8)
    }

    "drop 2" in {
      list = list.dropHead()
      list.depth shouldBe 5
      list.size shouldBe 7
      list.toSlice shouldBe Slice[KeyValue](range, 3, 4, 5, 6, 7, 8)

      list = list.dropHead()
      list.depth shouldBe 4
      list.size shouldBe 6
      list.toSlice shouldBe Slice[KeyValue](3, 4, 5, 6, 7, 8)

      list = list.dropHead()
      list = list.dropHead()
      list.depth shouldBe 3
      list.size shouldBe 4
      list.toSlice shouldBe Slice[KeyValue](5, 6, 7, 8)

      list = list.dropHead()
      list = list.dropHead()
      list = list.dropHead()
      list = list.dropHead()
      list.depth shouldBe 1
      list.size shouldBe 0
    }

    "merge when empty" in {
      list = list append list append list append list append MergeList(Slice[KeyValue](9))
      list.depth shouldBe 1
      list.size shouldBe 1
      list.toSlice shouldBe Slice[KeyValue](9)

      list.dropHead()
      list.depth shouldBe 1
      list.size shouldBe 0

      list = MergeList[KeyValue.Range, KeyValue](Slice[KeyValue](1)) append list append list append list append list
      list.depth shouldBe 1
      list.size shouldBe 1
      list.toSlice shouldBe Slice[KeyValue](1)

      list.dropHead()
      list.depth shouldBe 1
      list.size shouldBe 0
    }
  }
}
