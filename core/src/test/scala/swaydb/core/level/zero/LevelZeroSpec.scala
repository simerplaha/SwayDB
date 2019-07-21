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
//package swaydb.core.level.zero
//
//import org.scalamock.scalatest.MockFactory
//import org.scalatest.OptionValues._
//import swaydb.core.CommonAssertions._
//import swaydb.core.IOValues._
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.data.{Memory, Transient}
//import swaydb.core.io.file.IOEffect
//import swaydb.core.util.Benchmark
//import swaydb.core.{TestBase, TestTimer}
//import swaydb.data.compaction.Throttle
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//import swaydb.data.util.StorageUnits._
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//import scala.concurrent.duration._
//import scala.util.Random
//
//class LevelZeroSpec0 extends LevelZeroSpec
//
//class LevelZeroSpec1 extends LevelZeroSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegmentsOnWrite = true
//  override def mmapSegmentsOnRead = true
//  override def level0MMAP = true
//  override def appendixStorageMMAP = true
//}
//
//class LevelZeroSpec2 extends LevelZeroSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegmentsOnWrite = false
//  override def mmapSegmentsOnRead = false
//  override def level0MMAP = false
//  override def appendixStorageMMAP = false
//}
//
//class LevelZeroSpec3 extends LevelZeroSpec {
//  override def inMemoryStorage = true
//}
//
//sealed trait LevelZeroSpec extends TestBase with MockFactory with Benchmark {
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//  implicit val testTimer: TestTimer = TestTimer.Empty
//  implicit val timeOrder = TimeOrder.long
//
//  import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
//
//  val keyValuesCount = 10
//
//  //    override def deleteFiles = false
//
//  "LevelZero" should {
//    "initialise" in {
//      val nextLevel = TestLevel()
//      val zero = TestLevelZero(Some(nextLevel))
//      if (persistent) {
//        zero.existsOnDisk shouldBe true
//        nextLevel.existsOnDisk shouldBe true
//        //maps folder is initialised
//        IOEffect.exists(zero.path.resolve("0/0.log")) shouldBe true
//        zero.reopen.existsOnDisk shouldBe true
//      } else {
//        zero.existsOnDisk shouldBe false
//        nextLevel.existsOnDisk shouldBe false
//      }
//    }
//  }
//
//  "LevelZero.put" should {
//    "write key-value" in {
//      def assert(zero: LevelZero): Unit = {
//        zero.put(1, "one").runIO
//        zero.get(1).runIO.value.getOrFetchValue.runIO.value shouldBe ("one": Slice[Byte])
//
//        zero.put("2", "two").runIO
//        zero.get("2").runIO.value.getOrFetchValue.runIO.value shouldBe ("two": Slice[Byte])
//      }
//
//      val zero = TestLevelZero(Some(TestLevel(throttle = (_) => Throttle(10.seconds, 0))))
//      assert(zero)
//      if (persistent) assert(zero.reopen)
//    }
//
//    "write key-values that have empty bytes but the Slices are closed" in {
//      val level = TestLevel(throttle = (_) => Throttle(10.seconds, 0))
//      val zero = TestLevelZero(Some(level))
//      val one = Slice.create[Byte](10).addInt(1).close()
//
//      zero.put(one, one).runIO
//
//      val gotFromLevelZero = zero.get(one).runIO.value.getOrFetchValue.runIO.value
//      gotFromLevelZero shouldBe one
//      //ensure that key-values are not unsliced in LevelZero.
//      gotFromLevelZero.underlyingArraySize shouldBe 10
//
//      //the following does not apply to in-memory Levels
//      //in-memory key-values are slice of the whole Segment.
//      if (persistent) {
//        //put the same key-value to Level1 and expect the key-values to be sliced
//        level.putKeyValuesTest(Slice(Memory.put(one, one))).runIO
//        val gotFromLevelOne = level.get(one).runIO.value
//        gotFromLevelOne.getOrFetchValue.runIO.value shouldBe one
//        //ensure that key-values are not unsliced in LevelOne.
//        gotFromLevelOne.getOrFetchValue.runIO.value.underlyingArraySize shouldBe 4
//      }
//    }
//
//    "not write empty key-value" in {
//      val zero = TestLevelZero(Some(TestLevel()))
//      zero.put(Slice.empty, Slice.empty).failed.runIO.exception shouldBe a[IllegalArgumentException]
//    }
//
//    "write empty values" in {
//      val zero = TestLevelZero(Some(TestLevel()))
//      zero.put(1, Slice.empty).runIO
//      zero.get(1).safeGetBlocking.runIO.value.getOrFetchValue.runIO.value shouldBe Slice.empty
//    }
//
//    "write large keys and values and reopen the database and re-read key-values" in {
//      //approx 2 mb key and values
//
//      val key1 = "a" + Random.nextString(750000): Slice[Byte]
//      val key2 = "b" + Random.nextString(750000): Slice[Byte]
//
//      val value1 = Random.nextString(750000): Slice[Byte]
//      val value2 = Random.nextString(750000): Slice[Byte]
//
//      def assertWrite(zero: LevelZero): Unit = {
//        zero.put(key1, value1).runIO
//        zero.put(key2, value2).runIO
//      }
//
//      def assertRead(zero: LevelZero): Unit = {
//        zero.get(key1).runIO.value.getOrFetchValue.runIO.value shouldBe value1
//        zero.get(key2).runIO.value.getOrFetchValue.runIO.value shouldBe value2
//      }
//
//      val zero = TestLevelZero(Some(TestLevel(throttle = _ => Throttle(10.seconds, 0))))
//      assertWrite(zero)
//      assertRead(zero)
//
//      //allow compaction to do it's work
//      sleep(2.seconds)
//      if (persistent) assertRead(zero.reopen)
//    }
//
//    "write keys only" in {
//      val zero = TestLevelZero(Some(TestLevel()))
//
//      zero.put("one").runIO
//      zero.put("two").runIO
//
//      zero.get("one").safeGetBlocking.runIO.value.getOrFetchValue.runIO shouldBe None
//      zero.get("two").safeGetBlocking.runIO.value.getOrFetchValue.runIO shouldBe None
//
//      zero.contains("one").runIO shouldBe true
//      zero.contains("two").runIO shouldBe true
//      zero.contains("three").runIO shouldBe false
//    }
//
//    "batch write key-values" in {
//      val keyValues = randomIntKeyStringValues(keyValuesCount)
//
//      val zero = TestLevelZero(Some(TestLevel()))
//      zero.put(_ => keyValues.toMapEntry.get).runIO
//
//      assertGet(keyValues, zero)
//
//      zero.bloomFilterKeyValueCount.runIO shouldBe keyValues.size
//    }
//
//    "batch writing empty keys should fail" in {
//      if (persistent) {
//        val keyValues = Slice(Transient.put(Slice.empty, 1))
//
//        val zero = TestLevelZero(Some(TestLevel()))
//        assertThrows[Exception] {
//          zero.put(_ => keyValues.toMapEntry.get)
//        }
//      } else {
//        //Currently this test does not apply for in-memory. Empty keys should NEVER be written.
//        //Persistent batch writes check for empty keys but since in-memory is just a skipList in Level0, there is no
//        //check. The design of MapEntry restricts this which should be fixed.
//      }
//    }
//  }
//
//  "LevelZero.remove" should {
//    "remove key-values" in {
//      val zero = TestLevelZero(Some(TestLevel(throttle = (_) => Throttle(10.seconds, 0))), mapSize = 1.byte)
//      val keyValues = randomIntKeyStringValues(keyValuesCount)
//      keyValues foreach {
//        keyValue =>
//          zero.put(keyValue.key, keyValue.getOrFetchValue).runIO
//      }
//
//      if (unexpiredPuts(keyValues).nonEmpty)
//        zero.head.safeGetBlocking.get shouldBe defined
//
//      keyValues foreach {
//        keyValue =>
//          zero.remove(keyValue.key).runIO
//      }
//
//      zero.head.runIO shouldBe empty
//      zero.last.runIO shouldBe empty
//    }
//
//    "batch remove key-values" in {
//      val keyValues = randomIntKeyStringValues(keyValuesCount)
//      val zero = TestLevelZero(Some(TestLevel()))
//      zero.put(_ => keyValues.toMapEntry.get).runIO
//
//      assertGet(keyValues, zero)
//
//      val removeKeyValues = Slice(keyValues.map(keyValue => Memory.remove(keyValue.key)).toArray)
//      zero.put(_ => removeKeyValues.toMapEntry.get).runIO
//
//      assertGetNone(keyValues, zero)
//      zero.head.runIO shouldBe empty
//    }
//  }
//
//  "LevelZero.clear" should {
//    "a database with single key-value" in {
//      val zero = TestLevelZero(Some(TestLevel(throttle = (_) => Throttle(10.seconds, 0))), mapSize = 1.byte)
//      val keyValues = randomIntKeyStringValues(1)
//      keyValues foreach {
//        keyValue =>
//          zero.put(keyValue.key, keyValue.getOrFetchValue).runIO
//      }
//
//      zero.bloomFilterKeyValueCount.get shouldBe 1
//
//      zero.clear().safeGetBlocking.get
//
//      zero.head.runIO shouldBe empty
//      zero.last.runIO shouldBe empty
//    }
//
//    "remove all key-values" in {
//      val zero = TestLevelZero(Some(TestLevel(throttle = (_) => Throttle(10.seconds, 0))), mapSize = 1.byte)
//      val keyValues = randomIntKeyStringValues(keyValuesCount)
//      keyValues foreach {
//        keyValue =>
//          zero.put(keyValue.key, keyValue.getOrFetchValue).runIO
//      }
//
//      zero.clear().safeGetBlocking.get
//
//      zero.head.runIO shouldBe empty
//      zero.last.runIO shouldBe empty
//    }
//  }
//
//  "LevelZero.head" should {
//    "return the first key-value" in {
//      //disable throttle
//      val zero = TestLevelZero(Some(TestLevel(throttle = (_) => Throttle(10.seconds, 0))), mapSize = 1.byte)
//
//      zero.put(1, "one").runIO
//      zero.put(2, "two").runIO
//      zero.put(3, "three").runIO
//      zero.put(4, "four").runIO
//      zero.put(5, "five").runIO
//
//      zero.head.safeGetBlocking.runIO.value.getOrFetchValue.runIO.value shouldBe ("one": Slice[Byte])
//
//      //remove 1
//      zero.remove(1).runIO
//      println
//      zero.head.safeGetBlocking.runIO.value.getOrFetchValue.runIO.value shouldBe ("two": Slice[Byte])
//
//      zero.remove(2).runIO
//      zero.remove(3).runIO
//      zero.remove(4).runIO
//
//      zero.head.safeGetBlocking.runIO.value.getOrFetchValue.runIO.value shouldBe ("five": Slice[Byte])
//
//      zero.remove(5).runIO
//      zero.head.runIO shouldBe empty
//      zero.last.runIO shouldBe empty
//    }
//  }
//
//  "LevelZero.last" should {
//    "return the last key-value" in {
//      val zero = TestLevelZero(Some(TestLevel()), mapSize = 1.byte)
//
//      zero.put(1, "one").runIO
//      zero.put(2, "two").runIO
//      zero.put(3, "three").runIO
//      zero.put(4, "four").runIO
//      zero.put(5, "five").runIO
//
//      zero.last.safeGetBlocking.runIO.value.getOrFetchValue.runIO.value shouldBe ("five": Slice[Byte])
//
//      //remove 5
//      zero.remove(5).runIO
//      zero.last.safeGetBlocking.runIO.value.getOrFetchValue.runIO.get shouldBe ("four": Slice[Byte])
//
//      zero.remove(2).runIO
//      zero.remove(3).runIO
//      zero.remove(4).runIO
//
//      println
//      zero.last.safeGetBlocking.runIO.value.getOrFetchValue.runIO.value shouldBe ("one": Slice[Byte])
//
//      zero.remove(1).runIO
//      zero.last.runIO shouldBe empty
//      zero.head.runIO shouldBe empty
//    }
//  }
//
//  "LevelZero.remove range" should {
//    "not allow from key to be > than to key" in {
//      val zero = TestLevelZero(Some(TestLevel()), mapSize = 1.byte)
//      zero.remove(10, 1).failed.runIO.exception.getMessage shouldBe "fromKey should be less than or equal to toKey"
//      zero.remove(2, 1).failed.runIO.exception.getMessage shouldBe "fromKey should be less than or equal to toKey"
//    }
//  }
//
//  "LevelZero.update range" should {
//    "not allow from key to be > than to key" in {
//      val zero = TestLevelZero(Some(TestLevel()), mapSize = 1.byte)
//      zero.update(10, 1, value = "value").failed.runIO.exception.getMessage shouldBe "fromKey should be less than or equal to toKey"
//      zero.update(2, 1, value = "value").failed.runIO.exception.getMessage shouldBe "fromKey should be less than or equal to toKey"
//    }
//  }
//}
