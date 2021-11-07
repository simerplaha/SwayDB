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
//
//package swaydb.core.merge
//
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.IOValues._
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestData._
//import swaydb.core.TestTimer
//import swaydb.core.data.Memory
//import swaydb.testkit.RunThis._
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//
//class FunctionMerger_PendingApply_Spec extends AnyWordSpec with Matchers {
//
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//
//  "Merging Function into PendingApply with a single apply" when {
//
//    "times are in order" should {
//
//      "always return the same result as the Function being merged into Fixed" in {
//
//        implicit val testTimer = TestTimer.Incremental()
//
//        runThis(1000.times) {
//          val key = randomBytesSlice()
//
//          val apply = randomApplyWithDeadline()
//          //oldKeyValue but it has a newer time.
//          val oldKeyValue = Memory.PendingApply(key = key, Slice(apply))
//
//          //new but has older time than oldKeyValue
//          val newKeyValue = randomFunctionKeyValue(key = key)
//
//          val expected = FixedMerger(newKeyValue, apply.toMemory(key)).runRandomIO.right.value
//
//          //          println(s"newKeyValue: $newKeyValue")
//          //          println(s"old apply: $apply")
//          //          println(s"oldKeyValue: $oldKeyValue")
//          //          println(s"expected: $expected")
//          //          println(s"function: ${functionStore.get(newKeyValue.function)}")
//
//          assertMerge(
//            newKeyValue = newKeyValue,
//            oldKeyValue = oldKeyValue,
//            expected = expected.asInstanceOf[Memory.Fixed],
//            lastLevel = expected.asInstanceOf[Memory.Fixed].toLastLevelExpected
//          )
//        }
//      }
//    }
//  }
//
//  "Merging Function into PendingApply with multiple apply" when {
//
//    "times are in order" should {
//
//      "always return the same result as the Function being merged into Fixed" in {
//
//        implicit val testTimer = TestTimer.Incremental()
//
//        runThis(1000.times) {
//          val key = eitherOne(randomBytesSlice(), Slice.emptyBytes)
//
//          val oldApplies = (1 to randomIntMax(20)) map { _ => randomApplyWithDeadline() } toSlice
//
//          val newKeyValue = randomFunctionKeyValue(key = key)
//
//          val oldKeyValue = Memory.PendingApply(key = key, oldApplies)
//
//          val expected = collapseMerge(newKeyValue, oldApplies)
//
//          //          println(s"newKeyValue: $newKeyValue")
//          //          println(s"old applies: $oldApplies")
//          //          println(s"oldKeyValue: $oldKeyValue")
//          ////          println(s"function result: ${functionStore.get(newKeyValue.function).get.asInstanceOf[SwayFunction.Key].f(randomBytesSlice())}")
//          //          println(s"expected: $expected")
//          //          println
//
//          assertMerge(
//            newKeyValue = newKeyValue,
//            oldKeyValue = oldKeyValue,
//            expected = expected.asInstanceOf[Memory.Fixed],
//            lastLevel = expected.asInstanceOf[Memory.Fixed].toLastLevelExpected
//          )
//        }
//      }
//    }
//  }
//}
