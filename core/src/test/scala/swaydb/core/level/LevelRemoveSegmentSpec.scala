///*
// * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
//package swaydb.core.level
//
//import org.scalamock.scalatest.MockFactory
//import org.scalatest.PrivateMethodTester
//import swaydb.IOValues._
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.actor.{FileSweeper, MemorySweeper}
//import swaydb.core.level.zero.LevelZeroSkipListMerger
//import swaydb.core.{TestBase, TestSweeper, TestTimer}
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//import swaydb.data.util.StorageUnits._
//
//class LevelRemoveSegmentSpec0 extends LevelRemoveSegmentSpec
//
//class LevelRemoveSegmentSpec1 extends LevelRemoveSegmentSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegmentsOnWrite = true
//  override def mmapSegmentsOnRead = true
//  override def level0MMAP = true
//  override def appendixStorageMMAP = true
//}
//
//class LevelRemoveSegmentSpec2 extends LevelRemoveSegmentSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegmentsOnWrite = false
//  override def mmapSegmentsOnRead = false
//  override def level0MMAP = false
//  override def appendixStorageMMAP = false
//}
//
//class LevelRemoveSegmentSpec3 extends LevelRemoveSegmentSpec {
//  override def inMemoryStorage = true
//}
//
//sealed trait LevelRemoveSegmentSpec extends TestBase with MockFactory with PrivateMethodTester {
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//  implicit val testTimer: TestTimer = TestTimer.Empty
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//  val keyValuesCount = 100
//
//  //  override def deleteFiles: Boolean =
//  //    false
//
//  implicit val maxOpenSegmentsCacheImplicitLimiter: FileSweeper.Enabled = TestSweeper.fileSweeper
//  implicit val memorySweeperImplicitSweeper: Option[MemorySweeper.All] = TestSweeper.memorySweeperMax
//  implicit val skipListMerger = LevelZeroSkipListMerger
//
//  "removeSegments" should {
//    "remove segments from disk and remove them from appendix" in {
//      val level = TestLevel(segmentSize = 1.kb)
//      level.putKeyValuesTest(randomPutKeyValues(keyValuesCount)).runRandomIO.right.value
//
//      level.removeSegments(level.segmentsInLevel()).runRandomIO.right.value
//
//      level.isEmpty shouldBe true
//
//      if (persistent) {
//        level.segmentFilesOnDisk shouldBe empty
//        level.reopen.isEmpty shouldBe true
//      }
//    }
//  }
//}
