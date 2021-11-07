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
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestData._
//import swaydb.core.TestTimer
//import swaydb.testkit.RunThis._
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//class RemoveMerger_Update_Spec extends AnyWordSpec with Matchers {
//
//  implicit val keyOrder = KeyOrder.default
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//
//  "Merging remove into any other fixed key-value" when {
//
//    "times are in order" should {
//
//      "always return new key-value" in {
//
//        implicit val testTimer = TestTimer.Incremental()
//
//        runThis(1000.times) {
//          val key = randomStringOption
//
//          val oldKeyValue = randomUpdateKeyValue(key = key)(eitherOne(testTimer, TestTimer.Empty))
//
//          val newKeyValue = randomRemoveKeyValue(key = key)(eitherOne(testTimer, TestTimer.Empty))
//
//          val expected =
//            if (newKeyValue.deadline.isDefined)
//              oldKeyValue.copy(time = newKeyValue.time, deadline = newKeyValue.deadline)
//            else
//              newKeyValue
//
//          assertMerge(
//            newKeyValue = newKeyValue,
//            oldKeyValue = oldKeyValue,
//            expected = expected,
//            lastLevel = expected.toLastLevelExpected
//          )
//        }
//      }
//    }
//  }
//}
