///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.segment.format.a
//
//import java.nio.file._
//
//import org.scalatest.OptionValues._
//import swaydb.IOValues._
//import swaydb.core.CommonAssertions._
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.actor.MemorySweeper
//import swaydb.core.data.{Memory, _}
//import swaydb.core.io.file.BlockCache
//import swaydb.core.segment.Segment
//import swaydb.core.segment.format.a.block._
//import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
//import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
//import swaydb.core.util.Benchmark
//import swaydb.core.{TestBase, TestSweeper}
//import swaydb.data.config.{ActorConfig, MemoryCache}
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//import swaydb.data.util.StorageUnits._
//
//import scala.concurrent.duration._
//
///**
// * These class has tests to assert the behavior of [[MemorySweeper]] on [[swaydb.core.segment.Segment]]s.
// */
//class SegmentMemorySweeperSpec extends TestBase {
//
//  val keyValuesCount = 100
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//  implicit val memorySweeper = TestSweeper.memorySweeper10
//  implicit def blockCache: Option[BlockCache.State] = TestSweeper.randomBlockCache
//
//  //  override def deleteFiles = false
//
//  "PersistentSegment" should {
//    "drop Group key-value only after it's been decompressed" in {
//      //create a group
//      val groupKeyValues = randomKeyValues(10000)
//
//      //add key-values to the right of the group
//      val nonGroupKeyValues = randomKeyValues(count = 1000, addUpdates = true, startId = Some(groupKeyValues.last.key.readInt() + 1))
//
//      //set the limiter to drop key-values fast
//      implicit val memorySweeper: MemorySweeper.KeyValue =
//        MemorySweeper(MemoryCache.EnableKeyValueCache(group.stats.valueLength, ActorConfig.TimeLoop(2.seconds, ec)))
//          .value
//          .asInstanceOf[MemorySweeper.KeyValue]
//
//      try {
//
//        //create persistent Segment
//        val segment = TestSegment(mergedKeyValues)(KeyOrder.default, Some(memorySweeper), TestSweeper.fileSweeper, timeOrder, blockCache, SegmentIO.random).runRandomIO.right.value
//
//        //initially Segment's cache is empty
//        segment.areAllCachesEmpty shouldBe true
//
//        //read all key-values and this should trigger dropping of key-values
//        //read sequentially so that groups are added to the queue in sequential and also dropped.
//        Benchmark("Reading all key-values sequentially.") {
//          assertGetSequential(nonGroupKeyValues, segment)
//          assertGetSequential(groupKeyValues, segment)
//        }
//
//        //Group is cached into the Segment
//        val headGroup = segment.skipList.headKeyValue.get._2.asInstanceOf[Persistent.Group]
////        headGroup.isKeyValuesCacheEmpty shouldBe false
////        headGroup.areAllCachesEmpty shouldBe false
////        segment.isInKeyValueCache(headGroup.key) shouldBe true
//
//        //eventually all other key-values are dropped and the group remains.
//        eventual(4.seconds)(segment.cachedKeyValueSize shouldBe 1)
//
//        //fetch the head Group key-value from the Segment's cache and assert that it actually is decompressed and it's cache is empty.
//        val headGroupAgain = segment.skipList.headKeyValue.get._2.asInstanceOf[Persistent.Group]
//        eventual(4.seconds)(headGroupAgain.isBlockCacheEmpty shouldBe true)
//
//        //remove more key-values so that Group gets pushed out.
//        assertGet(nonGroupKeyValues.take(10), segment)
//        //expect group to removed entirely from the cache this time.
//        eventual(4.seconds)(segment.isInKeyValueCache(headGroup.key) shouldBe false)
//
//        segment.close.right.value
//      } finally {
//        memorySweeper.terminate()
//      }
//    }
//  }
//}
