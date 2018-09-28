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
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.util.Benchmark
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

//@formatter:off
class LevelHead0Spec extends LevelHeadSpec

class LevelHeadSpec1 extends LevelHeadSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelHeadSpec2 extends LevelHeadSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelHeadSpec3 extends LevelHeadSpec {
  override def inMemoryStorage = true
}
//@formatter:on

sealed trait LevelHeadSpec extends TestBase with MockFactory with Benchmark {

  override implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  implicit override val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomCompressionTypeOption(keyValuesCount)
  val keyValuesCount = 100

  "Level.head when lower Level is empty" should {
    "return None if the Level is empty" in {
      assertOnLevel(
        keyValues = Slice.empty,
        assertion = _.head.assertGetOpt shouldBe empty
      )
    }

    "return None if the Level has only a Remove key-value" in {
      assertOnLevel(
        keyValues = Slice(Memory.Remove(1)),
        assertion = _.head.assertGetOpt shouldBe empty
      )
    }

    "return None if the Level contains head Remove range with None fromValue" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 100, None, Value.Remove(None))),
        assertion = _.head.assertGetOpt shouldBe empty
      )
    }

    "return None if the Level contains head Remove range with Remove fromValue" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 100, Some(Value.Remove(None)), Value.Remove(None))),
        assertion = _.head.assertGetOpt shouldBe empty
      )
    }

    "return head if the Level contains head Put" in {
      assertOnLevel(
        keyValues = Slice(Memory.Put(100, 100)),
        assertion = _.head.assertGet shouldBe Memory.Put(100, 100)
      )
    }

    "return head if the Level contains head Remove Range with Put fromValue" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 100, Some(Value.Put(1)), Value.Remove(None))),
        assertion = _.head.assertGet shouldBe Memory.Put(1, 1)
      )
    }

    "return head if the Level contains head Put Range with Put rangeValue" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 100, Some(Value.Put(1)), Value.Update(100))),
        assertion = _.head.assertGet shouldBe Memory.Put(1, 1)
      )
    }

    "return head if the Level's first key-value is a Remove range and second key-value is Put" in {
      assertOnLevel(
        keyValues = Slice(Memory.Range(1, 100, Some(Value.Remove(None)), Value.Remove(None)), Memory.Put(101, 101)),
        assertion = _.head.assertGet shouldBe Memory.Put(101, 101)
      )
    }
  }

  "Level.head when both Levels are non-empty" should {
    "return second key-value from lower Level when it contains the lowest" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Put(100, 100)),
        lowerLevelKeyValues = Slice(Memory.Put(50, 50)),
        assertion = _.head.assertGet shouldBe Memory.Put(50, 50)
      )
    }

    "return second key-value from upper Level when it contains the lowest" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Put(50, 50)),
        lowerLevelKeyValues = Slice(Memory.Put(100, 100)),
        assertion = _.head.assertGet shouldBe Memory.Put(50, 50)
      )
    }

    "return second key-value from lower Level if the first head was removed by Remove range in upper Level" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Range(1, 100, Some(Value.Remove(None)), Value.Remove(None))),
        lowerLevelKeyValues = Slice(Memory.Put(50, 50), Memory.Put(200, 200)),
        assertion = _.head.assertGet shouldBe Memory.Put(200, 200)
      )
    }

    "return second key-value from lower Level if the first head was removed by Remove range in upper Level and the lowest == remove range's toKey" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Range(1, 100, Some(Value.Remove(None)), Value.Remove(None))),
        lowerLevelKeyValues = Slice(Memory.Put(50, 50), Memory.Put(100, 100)),
        assertion = _.head.assertGet shouldBe Memory.Put(100, 100)
      )
    }

    "return head if the Level's first key-value is an Update Range, second key-value is Put and lower Level contains a head key-value that falls within the range" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Range(1, 100, Some(Value.Remove(None)), Value.Update(100)), Memory.Put(101, 101)),
        lowerLevelKeyValues = Slice(Memory.Put(50, 50)),
        assertion = _.head.assertGet shouldBe Memory.Put(50, 100)
      )
    }

    "return head when lower level has head Range but upper level contains the next lowest Put" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Put(101, 101)),
        lowerLevelKeyValues = Slice(Memory.Range(1, 10, None, Value.Update(100))),
        assertion = _.head.assertGet shouldBe Memory.Put(101, 101)
      )
    }

    "return head when lower level has head Range with fromValue set" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Put(101, 101)),
        lowerLevelKeyValues = Slice(Memory.Range(1, 10, Some(Value.Put("one")), Value.Update(100))),
        assertion = _.head.assertGet shouldBe Memory.Put(1, Some("one"))
      )
    }

    "return head if the Level's first key-value is an Update Range second key-value is Put and lower Level contains a head Remove key-value that falls within the range" in {
      assertOnLevel(
        upperLevelKeyValues = Slice(Memory.Range(1, 100, Some(Value.Remove(None)), Value.Update(100)), Memory.Put(101, 101)),
        lowerLevelKeyValues = Slice(Memory.Remove(50)),
        assertion = _.head.assertGet shouldBe Memory.Put(101, 101)
      )
    }

    "return head if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a head Put key-value that is smaller than upper Level's minKey" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update(100)),
            Memory.Put(101, 101)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(0)),
        assertion = _.head.assertGet shouldBe Memory.Put(0)
      )
    }

    "return head if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a head Put key-value that does not falls within the range" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update(100)),
            Memory.Put(101, 101)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(49)),
        assertion = _.head.assertGet shouldBe Memory.Put(49)
      )
    }

    "return head if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a head Put key-value that is equal to range's from key" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update(100)),
            Memory.Put(101, 101)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(50, 50)),
        assertion = _.head.assertGet shouldBe Memory.Put(101, 101)
      )
    }

    "return head if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a head Put key-value that is 1 greater than range's from key" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update(100)),
            Memory.Put(101, 101)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(51, 51)),
        assertion = _.head.assertGet shouldBe Memory.Put(51, 100)
      )
    }

    "return head if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a head Put key-value that is equal to range's to key" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update("100 range")),
            Memory.Put(101, 101)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(100, 100)),
        assertion = _.head.assertGet shouldBe Memory.Put(100, 100)
      )
    }

    "return head if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a head Put key-value that is equal to levels last" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update("100 range")),
            Memory.Put(101, 101)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(101, "one oooo one")),
        assertion = _.head.assertGet shouldBe Memory.Put(101, 101)
      )
    }

    "return head if the Level's first key-value is an Put, Remove & Update Range second key-value is Put and lower Level contains a head Put key-value that is greater than levels last" in {
      assertOnLevel(
        upperLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update("100 range")),
            Memory.Put(101, 101)
          ),
        lowerLevelKeyValues =
          Slice(Memory.Put(102, 102)),
        assertion = _.head.assertGet shouldBe Memory.Put(101, 101)
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
        assertion = _.head.assertGet shouldBe Memory.Put(102, 102)
      )
    }

    "return head if upper Level is empty" in {
      assertOnLevel(
        upperLevelKeyValues = Slice.empty,
        lowerLevelKeyValues = Slice(Memory.Put(102, 102)),
        assertion = _.head.assertGet shouldBe Memory.Put(102, 102)
      )

      assertOnLevel(
        upperLevelKeyValues = Slice.empty,
        lowerLevelKeyValues =
          Slice(
            Memory.Remove(1),
            Memory.Range(50, 100, Some(Value.Remove(None)), Value.Update("100 range")),
            Memory.Put(101, 101)
          ),
        assertion = _.head.assertGet shouldBe Memory.Put(101, 101)
      )

    }
  }
}
