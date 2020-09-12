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
//package swaydb.core.data
//
//import swaydb.core.CommonAssertions._
//import swaydb.data.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.segment.format.a.block.SortedIndexBlock
//import swaydb.core.{TestBase, TestTimer}
//import swaydb.data.slice.Slice._
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//class TransientSpec extends TestBase {
//
//  val keyValueCount = 100
//
//  implicit def testTimer: TestTimer = TestTimer.random
//
//  "be reverse iterable" in {
//    //    val one = Memory.remove(1)
//    //    val two = Memory.remove(2, Some(one))
//    val three = Memory.put(key = 3, value = Some(3), previous = None)
//    //    val four = Memory.remove(4, Some(three))
//    val five = Memory.put(key = 5, value = Some(5), previous = Some(three))
//
//    //    five.reverseIterator.toList should contain inOrderOnly(five, four, three, two, one)
//  }
//
//  "hasSameValue" should {
//
//    "return false for put" in {
//      runThis(100.times) {
//        val left = randomPutKeyValue(1, None).toTransient
//        val right = randomFixedTransientKeyValue(randomString, Some(randomString))
//
//        if (right.isInstanceOf[Memory.Remove]) {
//          Memory.hasSameValue(left = left, right = right) shouldBe true
//          Memory.hasSameValue(left = right, right = left) shouldBe true
//        } else {
//          Memory.hasSameValue(left = left, right = right) shouldBe false
//          Memory.hasSameValue(left = right, right = left) shouldBe false
//        }
//      }
//    }
//  }
//
//  "enablePrefixCompression" should {
//    "return false is reset count is 0" in {
//      runThis(100.times) {
//        Memory.enablePrefixCompression(
//          randomFixedKeyValue(1)
//            .toTransient(
//              previous = None,
//              sortedIndexConfig =
//                SortedIndexBlock.Config.random.copy(prefixCompressionResetCount = 0)
//            )
//        ) shouldBe false
//      }
//    }
//
//    "return true for every 2nd key-value" in {
//      runThis(100.times, log = true) {
//        val keyValues =
//          Slice(
//            randomFixedKeyValue(1),
//            randomFixedKeyValue(2),
//            randomFixedKeyValue(3),
//            randomFixedKeyValue(4)
//          ).toTransient(
//            sortedIndexConfig =
//              SortedIndexBlock.Config.random.copy(
//                prefixCompressionResetCount = 2,
//                normaliseIndex = false
//              )
//          )
//
//        Memory.enablePrefixCompression(keyValues.head) shouldBe false //there is no previous
//        Memory.enablePrefixCompression(keyValues(1)) shouldBe false //reset
//        Memory.enablePrefixCompression(keyValues(2)) shouldBe true //not reset
//        Memory.enablePrefixCompression(keyValues(3)) shouldBe false //reset again
//      }
//    }
//  }
//
//  "normalise" should {
//    "returns indexEntry bytes of same size" in {
//      runThis(100.times, log = true) {
//
//        val keyValues =
//          (1 to 100) map {
//            _ =>
//              eitherOne(
//                randomFixedKeyValue(Int.MaxValue).toTransient,
//                randomRangeKeyValue(randomIntMax(1000), 100 + randomIntMax(1000)).toTransient
//              )
//          } updateStats
//
//        val normalisedKeyValues = Memory.normalise(keyValues)
//
//        val expectedSize = normalisedKeyValues.head.indexEntryBytes.size
//        println(s"expectedSize: $expectedSize")
//
//        normalisedKeyValues foreach {
//          keyValue =>
//            keyValue.indexEntryBytes.size shouldBe expectedSize
//        }
//      }
//    }
//  }
//}
