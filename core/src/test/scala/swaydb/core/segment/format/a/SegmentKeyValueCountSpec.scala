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

package swaydb.core.segment.format.a

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.IOAssert._
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

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

  implicit val keyOrder = KeyOrder.default

  def keyValuesCount: Int

  implicit def groupingStrategy: Option[KeyValueGroupingStrategyInternal] =
    randomGroupingStrategyOption(keyValuesCount)

  "Segment.keyValueCount" should {

    "return 1 when the Segment contains only 1 key-value" in {
      runThis(100.times) {
        assertSegment(
          keyValues = randomizedKeyValues(1, addRandomGroups = false),
          assert =
            (keyValues, segment) => {
              keyValues should have size 1
              segment.getHeadKeyValueCount().assertGet shouldBe 1
              segment.getBloomFilterKeyValueCount().assertGet shouldBe (if (keyValues.head.toTransient.isRange) 2 else 1)
            }
        )
      }
    }

    "return the number of randomly generated key-values where there are no Groups" in {
      runThis(10.times) {
        assertSegment(
          keyValues = randomizedKeyValues(keyValuesCount, addRandomGroups = false),
          assert =
            (keyValues, segment) => {
              segment.getHeadKeyValueCount().assertGet shouldBe keyValues.size
              segment.getBloomFilterKeyValueCount().assertGet shouldBe
                ((keyValues.count(_.toTransient.isRange) * 2) + keyValues.count(keyValue => !keyValue.toTransient.isRange))
            }
        )
      }
    }

    "return the number key-values in a single Group" in {
      runThis(10.times) {
        val groupsKeyValues = randomizedKeyValues(keyValuesCount, addRandomGroups = false)
        assertSegment(
          keyValues = Slice(randomGroup(groupsKeyValues)).toMemory,
          assert =
            (keyValues, segment) => {
              //if nested groups are created then getHeadKeyValueCount shouldBe 1
              if (segment.getAll().assertGet.head.isInstanceOf[KeyValue.ReadOnly.Group])
                segment.getHeadKeyValueCount().assertGet shouldBe 1
              else
                segment.getHeadKeyValueCount().assertGet shouldBe groupsKeyValues.size

              segment.getBloomFilterKeyValueCount().assertGet shouldBe groupsKeyValues.size
            }
        )
      }
    }

    "return the number key-values in nested Groups" in {
      runThis(10.times) {
        val group1KeyValues = randomizedKeyValues(keyValuesCount, addRandomGroups = false)
        val group1 = randomGroup(group1KeyValues)

        val group2KeyValues = randomizedKeyValues(keyValuesCount, startId = Some(group1.maxKey.maxKey.readInt() + 1), addRandomGroups = false)
        val group2 = randomGroup((Slice(group1) ++ group2KeyValues).updateStats)
        group2.stats.totalBloomFiltersItemsCount shouldBe (group1KeyValues.size + group2KeyValues.size)

        //group3 contains group2 as a child and group2 contains group1 as a child.
        val group3KeyValues = randomizedKeyValues(keyValuesCount, startId = Some(group2.maxKey.maxKey.readInt() + 1), addRandomGroups = false)
        val group3 = randomGroup((Slice(group2) ++ group3KeyValues).updateStats)
        group3.stats.totalBloomFiltersItemsCount shouldBe (group1KeyValues.size + group2KeyValues.size + group3KeyValues.size)

        val group4KeyValues = randomizedKeyValues(keyValuesCount, startId = Some(group3.maxKey.maxKey.readInt() + 1), addRandomGroups = false)
        val group4 = randomGroup(group4KeyValues)
        group4.stats.totalBloomFiltersItemsCount shouldBe group4KeyValues.size

        assertSegment(
          keyValues = Slice(group4).toMemory,
          assert = {
            (_, segment) => {
              segment.getBloomFilterKeyValueCount().assertGet shouldBe group4KeyValues.size
              segment.getHeadKeyValueCount().assertGet shouldBe 1
            }
          }
        )

        assertSegment(
          keyValues = Slice(group3).toMemory,
          assert =
            (keyValues, segment) => {
              segment.getBloomFilterKeyValueCount().assertGet shouldBe (group1KeyValues.size + group2KeyValues.size + group3KeyValues.size)
              segment.getHeadKeyValueCount().assertGet shouldBe 1
            }
        )

        assertSegment(
          keyValues = Slice(randomGroup(Slice(group3, group4).updateStats)).toMemory,
          assert =
            (keyValues, segment) => {
              segment.getBloomFilterKeyValueCount().assertGet shouldBe (group1KeyValues.size + group2KeyValues.size + group3KeyValues.size + group4KeyValues.size)
              segment.getHeadKeyValueCount().assertGet shouldBe 1
            }
        )
      }
    }
  }
}
