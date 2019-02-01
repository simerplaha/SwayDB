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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.merge

import swaydb.core.{TestBase, TestData, TestLimitQueues}
import swaydb.core.data.{KeyValue, Persistent}
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.segment.format.a.SegmentWriter
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.IOAssert._
import swaydb.core.RunThis._
import scala.collection.mutable.ListBuffer
import swaydb.data.order.KeyOrder

class SegmentGrouper_GroupKeyValues_Count_Spec extends SegmentGrouper_GroupKeyValues_Spec {
  val useCount = true
  val force = false
}

class SegmentGrouper_GroupKeyValues_Count_Force_Spec extends SegmentGrouper_GroupKeyValues_Spec {
  val useCount = true
  val force = true
}

class SegmentGrouper_GroupKeyValues_Size_Spec extends SegmentGrouper_GroupKeyValues_Spec {
  val useCount = false
  val force = false
}

class SegmentGrouper_GroupKeyValues_Size_Force_Spec extends SegmentGrouper_GroupKeyValues_Spec {
  val useCount = false
  val force = false
}

sealed trait SegmentGrouper_GroupKeyValues_Spec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val keyValueLimiter = TestLimitQueues.keyValueLimiter
  val keyValueCount = 100

  def useCount: Boolean

  def force: Boolean

  "groupKeyValues" should {
    "return None" when {
      "there are no key-values" in {
        SegmentGrouper.groupKeyValues(
          segmentKeyValues = ListBuffer.empty,
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
          force = force,
          groupingStrategy =
            if (useCount)
              KeyValueGroupingStrategyInternal.Count(
                count = 20,
                groupCompression = None,
                indexCompression = randomCompression(),
                valueCompression = randomCompression()
              )
            else
              KeyValueGroupingStrategyInternal.Size(
                size = 0.byte,
                groupCompression = None,
                indexCompression = randomCompression(),
                valueCompression = randomCompression()
              )
        ).assertGetOpt shouldBe empty
      }

      "there are not enough key-values" in {
        val keyValues = randomizedKeyValues(keyValueCount, addRandomGroups = false)
        val mutableKeyValues = ListBuffer.empty[KeyValue.WriteOnly]
        keyValues foreach (mutableKeyValues += _)

        val result =
          SegmentGrouper.groupKeyValues(
            segmentKeyValues = mutableKeyValues,
            bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
            force = force,
            groupingStrategy =
              if (useCount)
                KeyValueGroupingStrategyInternal.Count(
                  count = keyValues.size + 10,
                  groupCompression = None,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
              else
                KeyValueGroupingStrategyInternal.Size(
                  size = keyValues.last.stats.segmentSizeWithoutFooter + 1,
                  groupCompression = None,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
          )

        if (!force) {
          result.assertGetOpt shouldBe empty
          //no mutation occurs
          mutableKeyValues shouldBe keyValues
        } else {
          val (bytes, _) = SegmentWriter.write(Slice(result.assertGet), TestData.falsePositiveRate).assertGet
          readAll(bytes).assertGet.head.asInstanceOf[Persistent.Group].segmentCache.getAll().assertGet shouldBe keyValues
        }
      }

      "there are enough key-values but key compression's minimum requirement is not satisfied" in {
        val keyValues = randomizedKeyValues(keyValueCount, addRandomGroups = false)
        val mutableKeyValues = ListBuffer.empty[KeyValue.WriteOnly]
        keyValues foreach (mutableKeyValues += _)

        SegmentGrouper.groupKeyValues(
          segmentKeyValues = mutableKeyValues,
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
          force = force,
          groupingStrategy =
            if (useCount)
              KeyValueGroupingStrategyInternal.Count(
                count = keyValueCount - 2,
                groupCompression = None,
                indexCompression = randomCompressionLZ4OrSnappy(100),
                valueCompression = randomCompression()
              )
            else
              KeyValueGroupingStrategyInternal.Size(
                size = keyValues.last.stats.segmentSizeWithoutFooter + 1,
                groupCompression = None,
                indexCompression = randomCompressionLZ4OrSnappy(100),
                valueCompression = randomCompression()
              )
        ).assertGetOpt shouldBe empty

        //no mutation occurs
        mutableKeyValues shouldBe keyValues
      }

      "there are enough key-values but values compression's minimum requirement is not satisfied" in {
        val keyValues = randomizedKeyValues(keyValueCount, addRandomGroups = false)
        val mutableKeyValues = ListBuffer.empty[KeyValue.WriteOnly]
        keyValues foreach (mutableKeyValues += _)

        SegmentGrouper.groupKeyValues(
          segmentKeyValues = mutableKeyValues,
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
          force = force,
          groupingStrategy =
            if (useCount)
              KeyValueGroupingStrategyInternal.Count(
                count = keyValueCount - 2,
                groupCompression = None,
                indexCompression = randomCompression(),
                valueCompression = randomCompressionLZ4OrSnappy(100)
              )
            else
              KeyValueGroupingStrategyInternal.Size(
                size = keyValues.last.stats.segmentSizeWithoutFooter + 1,
                groupCompression = None,
                indexCompression = randomCompression(),
                valueCompression = randomCompressionLZ4OrSnappy(100)
              )
        ).assertGetOpt shouldBe empty

        //no mutation occurs
        mutableKeyValues shouldBe keyValues
      }

      "a Group exists without any un-grouped key-values" in {
        Seq(1, 10, 100) foreach {
          minCount =>
            val keyValue = randomGroup()
            val mutableKeyValues = ListBuffer(keyValue: KeyValue.WriteOnly)

            SegmentGrouper.groupKeyValues(
              segmentKeyValues = mutableKeyValues,
              bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
              force = force,
              groupingStrategy =
                if (useCount)
                  KeyValueGroupingStrategyInternal.Count(
                    count = minCount,
                    groupCompression = None,
                    indexCompression = randomCompression(),
                    valueCompression = randomCompression()
                  )
                else
                  KeyValueGroupingStrategyInternal.Size(
                    size = minCount,
                    groupCompression = None,
                    indexCompression = randomCompression(),
                    valueCompression = randomCompression()
                  )
            ).assertGetOpt shouldBe empty

            //no mutation occurs
            mutableKeyValues should have size 1
            mutableKeyValues.head shouldBe keyValue
        }
      }

      "multiple Groups exists without any un-grouped key-values" in {
        val group1 = randomGroup(randomizedKeyValues(keyValueCount))
        val group2 = randomGroup(randomizedKeyValues(keyValueCount, startId = Some(group1.maxKey.maxKey.readInt() + 1)))
        val group3 = randomGroup(randomizedKeyValues(keyValueCount, startId = Some(group2.maxKey.maxKey.readInt() + 1)))

        val keyValues = Seq(group1, group2, group3).updateStats
        val mutableKeyValues = ListBuffer(keyValues.toList: _*)

        Seq(1, 10, 100) foreach {
          minCount =>
            SegmentGrouper.groupKeyValues(
              segmentKeyValues = mutableKeyValues,
              bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
              force = force,
              groupingStrategy =
                if (useCount)
                  KeyValueGroupingStrategyInternal.Count(
                    count = minCount,
                    groupCompression = None,
                    indexCompression = randomCompression(),
                    valueCompression = randomCompression()
                  )
                else
                  KeyValueGroupingStrategyInternal.Size(
                    size = mutableKeyValues.last.stats.segmentSizeWithoutFooter,
                    groupCompression = None,
                    indexCompression = randomCompression(),
                    valueCompression = randomCompression()
                  )
            ).assertGetOpt shouldBe empty
        }

        //no mutation occurs
        mutableKeyValues shouldBe keyValues
      }

      "a Group exists and there are not enough key-values" in {
        val group = randomGroup()

        val otherKeyValues = randomizedKeyValues(20, startId = Some(group.maxKey.maxKey.readInt() + 1), addRandomGroups = false)
        val keyValues = (Seq(group) ++ otherKeyValues).updateStats
        val mutableKeyValues = ListBuffer(keyValues.toList: _*)

        val result =
          SegmentGrouper.groupKeyValues(
            segmentKeyValues = mutableKeyValues,
            bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
            force = force,
            groupingStrategy =
              if (useCount)
                KeyValueGroupingStrategyInternal.Count(
                  count = otherKeyValues.size + 1,
                  groupCompression = None,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
              else
                KeyValueGroupingStrategyInternal.Size(
                  size = otherKeyValues.last.stats.segmentSizeWithoutFooter + 100,
                  groupCompression = None,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
          )

        if (!force) {
          result.assertGetOpt shouldBe empty
          //no mutation occurs
          mutableKeyValues shouldBe keyValues
        } else {
          val (bytes, _) = SegmentWriter.write(Slice(result.assertGet).updateStats, TestData.falsePositiveRate).assertGet
          readAll(bytes).assertGet.head.asInstanceOf[Persistent.Group].segmentCache.getAll().assertGet shouldBe otherKeyValues
        }
      }

      "multiple Groups exists and there are not enough key-values" in {
        val group1 = randomGroup(randomizedKeyValues(keyValueCount))
        val group2 = randomGroup(randomizedKeyValues(keyValueCount, startId = Some(group1.maxKey.maxKey.readInt() + 1)))
        val group3 = randomGroup(randomizedKeyValues(keyValueCount, startId = Some(group2.maxKey.maxKey.readInt() + 1)))

        val otherKeyValues = randomizedKeyValues(19, startId = Some(group3.maxKey.maxKey.readInt() + 1), addRandomGroups = false)
        val keyValues = (Seq(group1, group2, group3) ++ otherKeyValues).updateStats
        val mutableKeyValues = ListBuffer(keyValues.toList: _*)

        val result =
          SegmentGrouper.groupKeyValues(
            segmentKeyValues = mutableKeyValues,
            bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
            force = force,
            groupingStrategy =
              if (useCount)
                KeyValueGroupingStrategyInternal.Count(
                  count = otherKeyValues.size + 1,
                  groupCompression = None,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
              else
                KeyValueGroupingStrategyInternal.Size(
                  size = otherKeyValues.last.stats.segmentSizeWithoutFooter + 100,
                  groupCompression = None,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
          )

        if (!force) {
          result.assertGetOpt shouldBe empty
          //no mutation occurs
          mutableKeyValues shouldBe keyValues
        } else {
          val (bytes, _) = SegmentWriter.write(Slice(result.assertGet).updateStats, TestData.falsePositiveRate).assertGet
          readAll(bytes).assertGet.head.asInstanceOf[Persistent.Group].segmentCache.getAll().assertGet shouldBe otherKeyValues
        }
      }

      "randomly generated key-values but grouping limit is not reached" in {
        runThis(100.times) {
          val keyValues = randomizedKeyValues(keyValueCount, addRandomGroups = false)
          val mutableKeyValues = ListBuffer.empty[KeyValue.WriteOnly]
          keyValues foreach (mutableKeyValues += _)

          SegmentGrouper.groupKeyValues(
            segmentKeyValues = mutableKeyValues,
            bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
            force = force,
            groupingStrategy =
              if (useCount)
                KeyValueGroupingStrategyInternal.Count(
                  count = keyValues.size,
                  groupCompression = None,
                  indexCompression = randomCompressionLZ4OrSnappy(Double.MaxValue),
                  valueCompression = randomCompressionLZ4OrSnappy(Double.MaxValue)
                )
              else
                KeyValueGroupingStrategyInternal.Size(
                  size = keyValues.last.stats.segmentSizeWithoutFooter - 1,
                  groupCompression = None,
                  indexCompression = randomCompressionLZ4OrSnappy(Double.MaxValue),
                  valueCompression = randomCompressionLZ4OrSnappy(Double.MaxValue)
                )
          )

        }
      }
    }

    "return Compressed group (min compression requirement is satisfied - Successfully grouped)" when {
      "there are key-values" in {
        val keyValues = randomizedKeyValues(20, addRandomGroups = false)
        val mutableKeyValues = ListBuffer.empty[KeyValue.WriteOnly]
        keyValues foreach (mutableKeyValues += _)

        SegmentGrouper.groupKeyValues(
          segmentKeyValues = mutableKeyValues,
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
          force = force,
          groupingStrategy =
            if (useCount)
              KeyValueGroupingStrategyInternal.Count(
                count = keyValues.size,
                groupCompression = None,
                indexCompression = randomCompression(),
                valueCompression = randomCompression()
              )
            else
              KeyValueGroupingStrategyInternal.Size(
                size = keyValues.last.stats.segmentSizeWithoutFooter,
                groupCompression = None,
                indexCompression = randomCompression(),
                valueCompression = randomCompression()
              )
        ).assertGet

        //all key-values are merged into one group.
        mutableKeyValues should have size 1
        val (bytes, _) = SegmentWriter.write(mutableKeyValues, TestData.falsePositiveRate).assertGet
        readAll(bytes).assertGet.head.asInstanceOf[Persistent.Group].segmentCache.getAll().assertGet shouldBe keyValues
      }

      "a Group exists with key-values" in {
        val group = randomGroup()
        val keyValues = randomizedKeyValues(20, startId = Some(group.maxKey.maxKey.readInt() + 1), addRandomGroups = false)
        val mutableKeyValues = ListBuffer((Seq(group) ++ keyValues).updateStats.toList: _*)

        SegmentGrouper.groupKeyValues(
          segmentKeyValues = mutableKeyValues,
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
          force = force,
          groupingStrategy =
            if (useCount)
              KeyValueGroupingStrategyInternal.Count(
                count = keyValues.size,
                groupCompression = None,
                indexCompression = randomCompression(),
                valueCompression = randomCompression()
              )
            else
              KeyValueGroupingStrategyInternal.Size(
                size = keyValues.last.stats.segmentSizeWithoutFooter - 100,
                groupCompression = None,
                indexCompression = randomCompression(),
                valueCompression = randomCompression()
              )
        ).assertGet

        mutableKeyValues should have size 2
        val (bytes, _) = SegmentWriter.write(mutableKeyValues, TestData.falsePositiveRate).assertGet
        val readGroups = readAll(bytes).assertGet
        readGroups.head.asInstanceOf[Persistent.Group] shouldBe group
        readGroups.last.asInstanceOf[Persistent.Group].segmentCache.getAll().assertGet shouldBe keyValues
      }

      "multiple Groups exists with key-values" in {
        val group1 = randomGroup(randomizedKeyValues(keyValueCount))
        val group2 = randomGroup(randomizedKeyValues(keyValueCount, startId = Some(group1.maxKey.maxKey.readInt() + 1)))
        val group3 = randomGroup(randomizedKeyValues(keyValueCount, startId = Some(group2.maxKey.maxKey.readInt() + 1)))
        val keyValues = randomizedKeyValues(20, startId = Some(group3.maxKey.maxKey.readInt() + 1), addRandomGroups = false)

        val mutableKeyValues = ListBuffer((Seq(group1, group2, group3) ++ keyValues).updateStats.toList: _*)

        SegmentGrouper.groupKeyValues(
          segmentKeyValues = mutableKeyValues,
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
          force = force,
          groupingStrategy =
            if (useCount)
              KeyValueGroupingStrategyInternal.Count(
                count = keyValues.size,
                groupCompression = None,
                indexCompression = randomCompression(),
                valueCompression = randomCompression()
              )
            else
              KeyValueGroupingStrategyInternal.Size(
                size = keyValues.last.stats.segmentSizeWithoutFooter - 10,
                groupCompression = None,
                indexCompression = randomCompression(),
                valueCompression = randomCompression()
              )
        ).assertGet

        mutableKeyValues should have size 4
        val (bytes, _) = SegmentWriter.write(mutableKeyValues, TestData.falsePositiveRate).assertGet
        val readGroups = readAll(bytes).assertGet
        readGroups.head.asInstanceOf[Persistent.Group] shouldBe group1
        readGroups(1).asInstanceOf[Persistent.Group] shouldBe group2
        readGroups(2).asInstanceOf[Persistent.Group] shouldBe group3
        readGroups.last.asInstanceOf[Persistent.Group].segmentCache.getAll().assertGet shouldBe keyValues
      }

      "randomly generated key-values but minimum compression requirement is met" in {
        runThis(100.times) {
          val keyValues = randomizedKeyValues(keyValueCount, addRandomGroups = false)
          val mutableKeyValues = ListBuffer.empty[KeyValue.WriteOnly]
          keyValues foreach (mutableKeyValues += _)

          SegmentGrouper.groupKeyValues(
            segmentKeyValues = mutableKeyValues,
            bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
            force = force,
            groupingStrategy =
              if (useCount)
                KeyValueGroupingStrategyInternal.Count(
                  count = keyValues.size,
                  groupCompression = None,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
              else
                KeyValueGroupingStrategyInternal.Size(
                  size =
                    eitherOne(
                      left = keyValues.last.stats.segmentSizeWithoutFooter,
                      right = keyValues.last.stats.segmentSizeWithoutFooter - randomIntMax(keyValues.last.stats.segmentSizeWithoutFooter)
                    ),
                  groupCompression = None,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
          ).assertGet

          //all key-values are merged into one group.
          mutableKeyValues should have size 1
          val (bytes, _) = SegmentWriter.write(mutableKeyValues, TestData.falsePositiveRate).assertGet
          readAll(bytes).assertGet.head.asInstanceOf[Persistent.Group].segmentCache.getAll().assertGet shouldBe keyValues
        }
      }
    }
  }

}
