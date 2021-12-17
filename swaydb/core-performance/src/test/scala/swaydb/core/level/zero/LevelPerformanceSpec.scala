///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
////
////package swaydb.core.level.zero
////
////import swaydb.core.CommonAssertions._
////import swaydb.core.TestData._
////import swaydb.core.segment.cache.sweeper.MemorySweeper
////import swaydb.core.segment.ReadState
////import swaydb.core.util.{Benchmark, SkipList}
////import swaydb.core.{TestBase, TestSweeper}
////import swaydb.slice.order.KeyOrder
////import swaydb.slice.Slice
////import swaydb.config.util.StorageUnits._
////
////import scala.util.Random
////
//////@formatter:off
////class LevelZeroPerformanceSpec0 extends LevelZeroPerformanceSpec {
////  override def mmapSegmentsOnWrite = false
////  override def mmapSegmentsOnRead = false
////}
////
////class LevelZeroPerformanceSpec1 extends LevelZeroPerformanceSpec {
////  override def levelFoldersCount = 10
////  override def mmapSegmentsOnWrite = true
////  override def mmapSegmentsOnRead = true
////  override def level0MMAP = MMAP.Enabled(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
////  override def appendixStorageMMAP = MMAP.Enabled(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
////}
////
////class LevelZeroPerformanceSpec2 extends LevelZeroPerformanceSpec {
////  override def levelFoldersCount = 10
////  override def mmapSegmentsOnWrite = false
////  override def mmapSegmentsOnRead = false
////  override def level0MMAP = MMAP.Disabled(forceSave = TestForceSave.channel())
////  override def appendixStorageMMAP = MMAP.Disabled(forceSave = TestForceSave.channel())
////}
////
////class LevelZeroPerformanceSpec3 extends LevelZeroPerformanceSpec {
////  override def inMemoryStorage = true
////}
//////@formatter:on
////
////sealed trait LevelZeroPerformanceSpec extends TestBase {
////
////  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
////
////  implicit val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] = TestSweeper.someMemorySweeper10
////  val keyValuesCount = 100
////
////  //  override def deleteFiles: Boolean = false
////
////  val keyValues = randomPutKeyValues(keyValuesCount, startId = Some(0), addPutDeadlines = false)
////  val shuffledKeyValues = Random.shuffle(keyValues)
////
////  def assertGet(level: LevelZero) = {
////    val readState = ReadState.limitHashMap(10, 2)
////    shuffledKeyValues foreach {
////      keyValue =>
////        //        val key = keyValue.key.readInt()
////        //        if (key % 1000 == 0)
////        //          println(s"Reading $key")
////        level.get(keyValue.key, readState).runIO.get.get
////      //        level.get(keyValue.key).runIO
////      //        val got = level.get(keyValue.key, readState).runIO.get.get
////      //        got shouldBe keyValue
////      //        got.getOrFetchValue shouldBe keyValue
////      //        println("value: " + level.get(keyValue.key).runIO._2.runIO.asInt())
////    }
////  }
////
////  def readLower(level: LevelZero) = {
////    val readState = ReadState.limitHashMap(10, 2)
////    (1 until keyValues.size) foreach {
////      index =>
////        //        println(s"index: $index")
////        level.lower(keyValues(index).key, readState)
////      //        val keyValue = level.lower(keyValues(index).key).runIO
////      //        keyValue.key shouldBe keyValues(index - 1).key
////      //        keyValue.getOrFetchValue.runIO.value shouldBe keyValues(index - 1).getOrFetchValue.runIO.value
////    }
////  }
////
////  def readHigher(level: LevelZero) =
////    (0 until keyValues.size - 1) foreach {
////      index =>
////      //        level.higher(keyValues(index).key)
////      //        val keyValue = level.higher(keyValues(index).key).runIO
////      //        keyValue.key shouldBe keyValues(index + 1).key
////      //        keyValue.getOrFetchValue.runIO.value shouldBe keyValues(index + 1).getOrFetchValue.runIO.value
////    }
////
////  var level =
////    TestLevelZero(
////      logSize = 8.mb,
////      nextLevel = None
////    )
////
////  keyValues foreach {
////    keyValue =>
////      level.put(keyValue.key, keyValue.getOrFetchValue)
////  }
////
////  //  def reopenLevelZero() = {
////  //    println("Re-opening LevelZero")
////  //    level.segmentsInLevelZero().foreach {
////  //      segment =>
////  //        segment.clearCachedKeyValues()
////  //        segment.close.runRandomIO.get
////  //    }
////  //    level = level.reopen
////  //  }
////
////  "benchmark skipList" in {
////    val skipList = SkipList.concurrent[Slice[Byte], Option[Slice[Byte]]]()(KeyOrder.default)
////
////    keyValues foreach {
////      keyValue =>
////        skipList.put(keyValue.key, keyValue.getOrFetchValue)
////    }
////
////    Benchmark("") {
////      shuffledKeyValues foreach {
////        keyValue =>
////          skipList.get(keyValue.key)
////      }
////    }
////  }
////
////  "LevelZero read performance 1" in {
////    Benchmark(s"read ${keyValues.size} key values when LevelZero persistent = ${levelStorage.persistent}, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
////      assertGet(level)
////    }
////
////    Benchmark(s"read ${keyValues.size} key values when LevelZero persistent = ${levelStorage.persistent}, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
////      assertGet(level)
////    }
////
////  }
////
////  "LevelZero read benchmark 2" in {
////    Benchmark(s"read ${keyValues.size} key values when LevelZero persistent = ${levelStorage.persistent}, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
////      assertGet(level)
////    }
////  }
////
////  "LevelZero read benchmark 3" in {
////    Benchmark(s"read ${keyValues.size} key values when LevelZero persistent = ${levelStorage.persistent}, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
////      readLower(level)
////    }
////  }
////
////  "LevelZero read benchmark 4" in {
////    //    if (levelStorage.persistent) reopenLevelZero()
////    Benchmark(s"read ${keyValues.size} key values when LevelZero persistent = ${levelStorage.persistent}, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
////      readLower(level)
////    }
////  }
////
////  "LevelZero read benchmark 5" in {
////    //    if (levelStorage.persistent) reopenLevelZero()
////    Benchmark(s"read ${keyValues.size} key values when LevelZero persistent = ${levelStorage.persistent}, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
////      readHigher(level)
////    }
////  }
////
////  "LevelZero read benchmark 6" in {
////    Benchmark(s"read ${keyValues.size} key values when LevelZero persistent = ${levelStorage.persistent}, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
////      readHigher(level)
////    }
////  }
////}
