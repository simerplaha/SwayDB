/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import swaydb.core.TestBase
import swaydb.core.data.{Memory, Value}
import swaydb.core.util.Benchmark
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

//@formatter:off
class LevelLastSpec1 extends LevelLastSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelLastSpec2 extends LevelLastSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelLastSpec3 extends LevelLastSpec {
  override def inMemoryStorage = true
}
//@formatter:on

class LevelLastSpec extends TestBase with MockFactory with Benchmark {

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  val keyValuesCount = 100

  "Level.last when lower Level is empty" should {
    "return None if the Level is empty" in {
      assertOnLevel(
        keyValues = Slice.empty,
        assertion = _.last.assertGetOpt shouldBe empty
      )
    }

    "return None if the Level has only a Remove key-value" in {
      assertOnLevel(
        keyValues = Slice(Memory.Remove(1)),
        assertion = _.last.assertGetOpt shouldBe empty
      )
    }

    "return None if the Level contains last Remove range with None fromValue" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 100, None, Value.Remove(None))),
        assertion = _.last.assertGetOpt shouldBe empty
      )
    }

    "return None if the Level contains last Remove range with Remove fromValue" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 100, Some(Value.Remove(None)), Value.Remove(None))),
        assertion = _.last.assertGetOpt shouldBe empty
      )
    }

    "return last if the Level contains last Put" in {
      assertOnLevel(
        keyValues = Slice(Memory.Put(100, 100)),
        assertion = _.last.assertGet shouldBe Memory.Put(100, 100)
      )
    }

    "return last if the Level contains last Remove Range with Put fromValue" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 100, Some(Value.Put(1)), Value.Remove(None))),
        assertion = _.last.assertGet shouldBe Memory.Put(1, 1)
      )
    }

    "return last if the Level contains last Put Range with Put rangeValue" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 100, Some(Value.Put(1)), Value.Update(100))),
        assertion = _.last.assertGet shouldBe Memory.Put(1, 1)
      )
    }

    "return last if the Level's first key-value is a Remove range and second key-value is Put" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 100, Some(Value.Remove(None)), Value.Remove(None)), Memory.Put(101, 101)),
        assertion = _.last.assertGet shouldBe Memory.Put(101, 101)
      )
    }
  }

  "Level.last when both Levels are non-empty" should {
    "return second key-value from upper Level when it contains the highest" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Put(100, 100)),
        lowerLevelKeyValues = Slice(Memory.Put(50, 50)),
        assertion = _.last.assertGet shouldBe Memory.Put(100, 100)
      )
    }

    "return second key-value from lower Level when it contains the highest" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Put(50, 50)),
        lowerLevelKeyValues = Slice(Memory.Put(100, 100)),
        assertion = _.last.assertGet shouldBe Memory.Put(100, 100)
      )
    }

    "return second key-value from lower Level if the first last was removed by Remove range in upper Level" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Range(1, 100, Some(Value.Remove(None)), Value.Remove(None))),
        lowerLevelKeyValues = Slice(Memory.Put(50, 50), Memory.Put(200, 200)),
        assertion = _.last.assertGet shouldBe Memory.Put(200, 200)
      )
    }

    "return second key-value from lower Level if the first last was removed by Remove range in upper Level and the highest == remove range's toKey" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Range(1, 100, Some(Value.Remove(None)), Value.Remove(None))),
        lowerLevelKeyValues = Slice(Memory.Put(50, 50), Memory.Put(100, 100)),
        assertion = _.last.assertGet shouldBe Memory.Put(100, 100)
      )
    }

    "return last if the Level's first key-value is an Update Range, second key-value is Put and lower Level contains a last key-value that falls within the range" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Range(1, 100, Some(Value.Remove(None)), Value.Update(100)), Memory.Put(101, 101)),
        lowerLevelKeyValues = Slice(Memory.Put(50, 50)),
        assertion = _.last.assertGet shouldBe Memory.Put(101, 101)
      )
    }

    "return last if the Level's last key-value is an Update Range with fromValue set to Remove and lower level contains an range key-value" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Put(10, 10), Memory.Range(20, 100, Some(Value.Remove(None)), Value.Update(100))),
        lowerLevelKeyValues = Slice(Memory.Put(90, 90)),
        assertion = _.last.assertGet shouldBe Memory.Put(90, 100)
      )
    }

    "return last if the Level's last key-value is an Update Range and lower Level contains a higher key-value that is not in Range" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Put(10, 10), Memory.Range(20, 100, None, Value.Update(100))),
        lowerLevelKeyValues = Slice(Memory.Put(15, 15)),
        assertion = _.last.assertGet shouldBe Memory.Put(15, 15)
      )
    }

    "return last when lower level has last Range but upper level contains the next highest Put" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Put(101, 101)),
        lowerLevelKeyValues = Slice(Memory.Range(1, 10, None, Value.Update(100))),
        assertion = _.last.assertGet shouldBe Memory.Put(101, 101)
      )
    }

    "return last when lower level has last Range with fromValue set" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Put(0, "zero")),
        lowerLevelKeyValues = Slice(Memory.Range(1, 10, Some(Value.Put("one")), Value.Update(100))),
        assertion = _.last.assertGet shouldBe Memory.Put(1, Some("one"))
      )
    }

    "return last if the Level's first key-value is an Update Range second key-value is Put and lower Level contains a last Remove key-value that falls within the range" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Range(1, 100, Some(Value.Remove(None)), Value.Update(100)), Memory.Put(101, 101)),
        lowerLevelKeyValues = Slice(Memory.Remove(50)),
        assertion = _.last.assertGet shouldBe Memory.Put(101, 101)
      )
    }

    "return last if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a last Put key-value that is smaller than upper Level's minKey" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update(100)),
            Memory.Put(101, 101)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(102)),
        assertion = _.last.assertGet shouldBe Memory.Put(102)
      )
    }

    "return last if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a last Put key-value that does not falls within the range" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update(100)),
            Memory.Put(101, 101)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(49)),
        assertion = _.last.assertGet shouldBe Memory.Put(101, 101)
      )
    }

    "return last if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a last Put key-value that is equal to range's from key" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update(100)),
            Memory.Put(101, 101)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(50, 50)),
        assertion = _.last.assertGet shouldBe Memory.Put(101, 101)
      )
    }

    "return last if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a last Put key-value that is 1 greater than range's from key" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update(100))
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(51, 51)),
        assertion = _.last.assertGet shouldBe Memory.Put(51, 100)
      )
    }

    "return last if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a last Put key-value that is equal to range's to key" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 99, Some(Value.Remove(None)), Value.Update("99 range")),
            Memory.Put(99, 99)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(100, 100)),
        assertion = _.last.assertGet shouldBe Memory.Put(100, 100)
      )
    }

    "return last if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a last Put key-value that is equal to levels last" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update("100 range")),
            Memory.Put(101, 101)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(101, "one oooo one")),
        assertion = _.last.assertGet shouldBe Memory.Put(101, 101)
      )
    }

    "return last if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a last Put key-value that is greater than levels last" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update("100 range")),
            Memory.Put(101, 101)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(102, 102)),
        assertion = _.last.assertGet shouldBe Memory.Put(102, 102)
      )

      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update("100 range")),
            Memory.Remove(101)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(102, 102)),
        assertion = _.last.assertGet shouldBe Memory.Put(102, 102)
      )
    }

    "return last if upper Level is empty" in {
      assertOnLevel(
        upperLevelKeyValues = Slice.empty,
        lowerLevelKeyValues = Slice(Memory.Put(102, 102)),
        assertion = _.last.assertGet shouldBe Memory.Put(102, 102)
      )

      assertOnLevel(
        upperLevelKeyValues = Slice.empty,
        lowerLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update("100 range")),
            Memory.Put(101, 101)
          ),
        assertion = _.last.assertGet shouldBe Memory.Put(101, 101)
      )

    }
  }
}
