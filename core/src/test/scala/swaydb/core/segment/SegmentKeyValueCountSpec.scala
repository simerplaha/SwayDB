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

package swaydb.core.segment

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.core.TestBase
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

//@formatter:off
class SegmentKeyValueCount0 extends SegmentKeyValueCount {
  val keyValuesCount = 1000
}

class SegmentKeyValueCount1 extends SegmentKeyValueCount {
  val keyValuesCount = 1000
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentKeyValueCount2 extends SegmentKeyValueCount {
  val keyValuesCount = 1000
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentKeyValueCount3 extends SegmentKeyValueCount {
  val keyValuesCount = 10000
  override def inMemoryStorage = true
}
//@formatter:on

sealed trait SegmentKeyValueCount extends TestBase with ScalaFutures with PrivateMethodTester {

  override implicit val ordering = KeyOrder.default

  def keyValuesCount: Int

  implicit override val groupingStrategy: Option[KeyValueGroupingStrategyInternal] =
    randomCompressionTypeOption(keyValuesCount)

  "Segment.keyValueCount" should {

    "return 1 when the Segment contains only 1 key-value" in {
      assertOnSegment(
        keyValues = Slice(Memory.Put(1, randomStringOption, randomDeadlineOption)),
        assertion = _.getKeyValueCount().assertGet shouldBe 1
      )
    }

    "return the number of randomly generated key-values where there are no Groups" in {
      assertOnSegment(
        keyValues = randomizedIntKeyValues(keyValuesCount, addRandomGroups = false),
        assertionWithKeyValues =
          (keyValues, segment) =>
            segment.getKeyValueCount().assertGet shouldBe keyValues.size
      )
    }

    "return the number key-values in a single Group" in {
      val groupsKeyValues = randomizedIntKeyValues(keyValuesCount, addRandomGroups = false)
      assertOnSegment(
        keyValues = Slice(randomGroup(groupsKeyValues)).toMemory,
        assertion = _.getKeyValueCount().assertGet shouldBe groupsKeyValues.size
      )
    }

    "return the number key-values in nested Groups" in {
      val group1KeyValues = randomizedIntKeyValues(keyValuesCount, addRandomGroups = false)
      val group1 = randomGroup(group1KeyValues)

      val group2KeyValues = randomizedIntKeyValues(keyValuesCount, startId = Some(group1.maxKey.maxKey.readInt() + 1), addRandomGroups = false)
      val group2 = randomGroup((Slice(group1) ++ group2KeyValues).updateStats)
      group2.stats.bloomFilterItemsCount shouldBe (group1KeyValues.size + group2KeyValues.size)

      //group3 contains group2 as a child and group2 contains group1 as a child.
      val group3KeyValues = randomizedIntKeyValues(keyValuesCount, startId = Some(group2.maxKey.maxKey.readInt() + 1), addRandomGroups = false)
      val group3 = randomGroup((Slice(group2) ++ group3KeyValues).updateStats)
      group3.stats.bloomFilterItemsCount shouldBe (group1KeyValues.size + group2KeyValues.size + group3KeyValues.size)

      val group4KeyValues = randomizedIntKeyValues(keyValuesCount, startId = Some(group3.maxKey.maxKey.readInt() + 1), addRandomGroups = false)
      val group4 = randomGroup(group4KeyValues)
      group4.stats.bloomFilterItemsCount shouldBe group4KeyValues.size

      assertOnSegment(
        keyValues = Slice(group4).toMemory,
        assertion = _.getKeyValueCount().assertGet shouldBe group4KeyValues.size
      )

      assertOnSegment(
        keyValues = Slice(group3).toMemory,
        assertion = _.getKeyValueCount().assertGet shouldBe (group1KeyValues.size + group2KeyValues.size + group3KeyValues.size)
      )

      assertOnSegment(
        keyValues = Slice(randomGroup(Slice(group3, group4).updateStats)).toMemory,
        assertion = _.getKeyValueCount().assertGet shouldBe (group1KeyValues.size + group2KeyValues.size + group3KeyValues.size + group4KeyValues.size)
      )
    }
  }
}