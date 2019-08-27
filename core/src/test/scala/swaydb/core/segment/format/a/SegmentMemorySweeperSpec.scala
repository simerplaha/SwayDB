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

import java.nio.file._

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.actor.MemorySweeper
import swaydb.core.data.{Memory, _}
import swaydb.core.io.file.BlockCache
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.block._
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.config.{ActorConfig, MemoryCache}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

import scala.concurrent.duration._

/**
 * These class has tests to assert the behavior of [[MemorySweeper]] on [[swaydb.core.segment.Segment]]s.
 */
class SegmentMemorySweeperSpec extends TestBase {

  val keyValuesCount = 100
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val memorySweeper = TestLimitQueues.memorySweeper
  implicit def blockCache: Option[BlockCache.State] = TestLimitQueues.randomBlockCache

  //  override def deleteFiles = false

  "MemorySegment" should {
    /**
     * Test that [[Memory.Group]] key-values are never dropped from the [[swaydb.core.segment.MemorySegment]]'s cache.
     * They should only be uncompressed.
     */
    "not drop head Group on memory-overflow" in {
      //create a group
      val groupKeyValues = randomizedKeyValues(1000, addGroups = false)
      val group =
        Transient.Group(
          keyValues = groupKeyValues,
          previous = None,
          groupConfig = SegmentBlock.Config.random,
          valuesConfig = ValuesBlock.Config.random,
          sortedIndexConfig = SortedIndexBlock.Config.random,
          binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
          hashIndexConfig = HashIndexBlock.Config.random,
          bloomFilterConfig = BloomFilterBlock.Config.random,
          createdInLevel = randomIntMax()
        ).runRandomIO.right.value

      //add more key-values to the right of the Group
      val nonGroupKeyValues = randomKeyValues(count = 1000, addUpdates = true, startId = Some(groupKeyValues.last.key.readInt() + 1))

      //key-values to write to the Segment
      val mergedKeyValues = (Seq(group) ++ nonGroupKeyValues).updateStats

      //set the limiter to drop key-values fast
      implicit val memorySweeper: MemorySweeper.KeyValue =
        MemorySweeper(MemoryCache.EnableKeyValueCache(1.byte, ActorConfig.TimeLoop(10000, 10000, 100.millisecond, ec)))
          .value
          .asInstanceOf[MemorySweeper.KeyValue]

      try {
        val segment =
          Segment.memory(
            path = Paths.get("/test"),
            createdInLevel = 0,
            keyValues = mergedKeyValues
          )(KeyOrder.default, timeOrder, functionStore, TestLimitQueues.fileSweeper, None, Some(memorySweeper), SegmentIO.random).runRandomIO.right.value

        //perform reads multiple times and assert that while the key-values are getting drop, the group key-value does
        //not value dropped
        runThis(100.times, log = true) {
          eventual(40.seconds) {
            //cache should only contain the uncompressed Group and other non group key-values
            segment.skipList.size shouldBe (nonGroupKeyValues.size + 1)
            segment.areAllCachesEmpty shouldBe false //group and other key-values exists

            //assert that group always exists and that it does not value dropped from the cache.
            def headGroup = segment.skipList.headKeyValue.get._2.asInstanceOf[Memory.Group]

            //read all key-values and this should trigger dropping of key-values
            assertGet(nonGroupKeyValues, segment)
            assertGet(groupKeyValues, segment)

            segment.isInKeyValueCache(headGroup.key) shouldBe true
            segment.isKeyValueCacheEmpty shouldBe false
            segment.areAllCachesEmpty shouldBe false
            //read more data and still they Group exists.
            assertGet(nonGroupKeyValues, segment)
            segment.isInKeyValueCache(headGroup.key) shouldBe true
            segment.areAllCachesEmpty shouldBe false

            eventual(30.seconds)(
              headGroup.areAllCachesEmpty shouldBe true
            )

            //but Segment's cache is never emptied
            segment.skipList.size shouldBe (nonGroupKeyValues.size + 1)
          }
        }
      } finally {
        memorySweeper.terminate()
      }
    }
  }

  "PersistentSegment" should {
    "drop Group key-value only after it's been decompressed" in {
      //create a group
      val groupKeyValues = randomKeyValues(10000)
      val group =
        Transient.Group(
          keyValues = groupKeyValues,
          previous = None,
          groupConfig = SegmentBlock.Config.random,
          valuesConfig = ValuesBlock.Config.random,
          sortedIndexConfig = SortedIndexBlock.Config.random,
          binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
          hashIndexConfig = HashIndexBlock.Config.random,
          bloomFilterConfig = BloomFilterBlock.Config.random,
          createdInLevel = randomIntMax()
        ).runRandomIO.right.value

      //add key-values to the right of the group
      val nonGroupKeyValues = randomKeyValues(count = 1000, addUpdates = true, startId = Some(groupKeyValues.last.key.readInt() + 1))

      //Segment's key-values
      val mergedKeyValues = (Seq(group) ++ nonGroupKeyValues).updateStats

      //set the limiter to drop key-values fast
      implicit val memorySweeper: MemorySweeper.KeyValue =
        MemorySweeper(MemoryCache.EnableKeyValueCache(group.stats.valueLength, ActorConfig.TimeLoop(10000, 10000, 2.seconds, ec)))
          .value
          .asInstanceOf[MemorySweeper.KeyValue]

      try {

        //create persistent Segment
        val segment = TestSegment(mergedKeyValues)(KeyOrder.default, Some(memorySweeper), TestLimitQueues.fileSweeper, timeOrder, blockCache, SegmentIO.random).runRandomIO.right.value

        //initially Segment's cache is empty
        segment.areAllCachesEmpty shouldBe true

        //read all key-values and this should trigger dropping of key-values
        assertGet(nonGroupKeyValues, segment)
        assertGet(groupKeyValues, segment)

        //Group is cached into the Segment
        val headGroup = segment.skipList.headKeyValue.get._2.asInstanceOf[Persistent.Group]
        headGroup.isKeyValuesCacheEmpty shouldBe false
        headGroup.areAllCachesEmpty shouldBe false
        segment.isInKeyValueCache(headGroup.key) shouldBe true

        //eventually all other key-values are dropped and the group remains.
        eventual(2.seconds)(segment.cachedKeyValueSize shouldBe 1)

        //fetch the head Group key-value from the Segment's cache and assert that it actually is decompressed and it's cache is empty.
        val headGroupAgain = segment.skipList.headKeyValue.get._2.asInstanceOf[Persistent.Group]
        eventual(2.seconds)(headGroupAgain.isBlockCacheEmpty shouldBe true)

        //remove more key-values so that Group gets pushed out.
        assertGet(nonGroupKeyValues.take(10), segment)
        //expect group to removed entirely from the cache this time.
        eventual(4.seconds)(segment.isInKeyValueCache(headGroup.key) shouldBe false)

        segment.close.right.value
      } finally {
        memorySweeper.terminate()
      }
    }
  }
}
