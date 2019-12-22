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
//package swaydb.core.merge
//
//import org.scalatest.{Matchers, WordSpec}
//import swaydb.Error.Segment.ExceptionHandler
//import swaydb.IO
//import swaydb.core.CommonAssertions._
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.TestTimer
//import swaydb.core.data.Memory
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//class PendingApplyMergerSpec extends WordSpec with Matchers {
//
//  implicit val keyOrder = KeyOrder.default
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//  "Merging PendingApply into any other fixed key-value" when {
//    "times are in order" in {
//
//      implicit val testTimer = TestTimer.Incremental()
//
//      runThis(1000.times) {
//        val key = randomStringOption
//
//        val oldKeyValue = randomPendingApplyKeyValue(key = key)
//
//        val newKeyValue = randomFixedKeyValue(key = key)
//        val expected = collapseMerge(newKeyValue, oldKeyValue.applies).asInstanceOf[Memory.Fixed]
//
//        //        println(s"oldKeyValue: $oldKeyValue")
//        //        println(s"newKeyValue: $newKeyValue")
//        //        println
//
//        assertMerge(
//          newKeyValue = newKeyValue,
//          oldKeyValue = oldKeyValue,
//          expected = expected,
//          lastLevelExpect = expected.toLastLevelExpected
//        )
//      }
//    }
//  }
//
//  "Merging PendingApply into any other fixed key-value" when {
//    "times are not in order" should {
//
//      "always return old key-value" in {
//
//        implicit val testTimer = TestTimer.Incremental()
//
//        runThis(1000.times) {
//          val key = randomStringOption
//
//          val newKeyValue = randomFixedKeyValue(key = key)
//
//          val oldKeyValue = randomPendingApplyKeyValue(key = key)
//
//          IO {
//            //if applies were return the the result can contain just the inner single apply key-value.
//            val expected = oldKeyValue.applies.head.toMemory(key)
//            assertMerge(
//              newKeyValue = newKeyValue,
//              oldKeyValue = oldKeyValue,
//              expected = expected,
//              lastLevelExpect = expected.toLastLevelExpected
//            )
//          } getOrElse {
//            //or else if applies were not read then the PendingApply is returned.
//            assertMerge(
//              newKeyValue = newKeyValue,
//              oldKeyValue = oldKeyValue,
//              expected = oldKeyValue,
//              lastLevelExpect = oldKeyValue.toLastLevelExpected
//            )
//          }
//        }
//      }
//    }
//  }
//}
