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
//package swaydb.core.level.seek
//
//import org.scalamock.scalatest.MockFactory
//import org.scalatest.OptionValues
//import org.scalatest.matchers.should.Matchers._
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.effect.IOValues._
//import swaydb.core.CommonAssertions._
//import swaydb.core.CoreTestData._
//import swaydb.core.segment.data.merge.{FixedMerger, FunctionMerger, PendingApplyMerger}
//import swaydb.core.segment.data.{SegmentFunctionOutput, Value}
//import swaydb.core.segment.ref.search.ThreadReadState
//import swaydb.core.{CoreTestData, TestTimer}
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.slice.Slice
//import swaydb.slice.order.{KeyOrder, TimeOrder}
//import swaydb.testkit.RunThis._
//import swaydb.testkit.TestKit._
//
//class GetSomeSpec extends AnyWordSpec with OptionValues {
//
//  implicit val keyOrder = KeyOrder.default
//  implicit val timeOrder = TimeOrder.long
//  implicit val functionStore = CoreTestData.functionStore
//
//  "return Some" when {
//    "put is not expired" in {
//      runThis(10.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        val keyValue = randomPutKeyValue(1, deadline = randomDeadlineOption(false))
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning keyValue
//
//        Get(1, ThreadReadState.random) shouldBe keyValue
//      }
//    }
//
//    "remove has time left" in {
//      runThis(10.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        val remove = randomRemoveKeyValue(1, Some(randomDeadline(expired = false)))
//        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(expired = false))
//        val expect = put.copy(deadline = remove.deadline.orElse(put.deadline), time = remove.time)
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning remove
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning put
//
//        Get(1, ThreadReadState.random) shouldBe expect
//      }
//    }
//
//    "update has time left" in {
//      runThis(10.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        val update = randomUpdateKeyValue(1, deadline = randomDeadlineOption(expired = false))
//        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(expired = false))
//        val expect = put.copy(deadline = update.deadline.orElse(put.deadline), value = update.value, time = update.time)
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning update
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning put
//
//        Get(1, ThreadReadState.random) shouldBe expect
//      }
//    }
//
//    "functions either update or update expiry but do not remove" in {
//      runThis(10.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        val function =
//          randomKeyValueFunctionKeyValue(
//            key = 1,
//            output =
//              eitherOne(
//                SegmentFunctionOutput.Expire(randomDeadline(false)),
//                SegmentFunctionOutput.Update(genStringSliceOptional(), randomDeadlineOption(false))
//              )
//          )
//
//        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(expired = false))
//        val expect = FunctionMerger(function, put).runRandomIO.get
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning function
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning put
//
//        Get(1, ThreadReadState.random) shouldBe expect
//      }
//    }
//
//    "pending applies that do not expire or remove" in {
//      runThis(10.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        val pendingApply =
//          randomPendingApplyKeyValue(
//            key = 1,
//            deadline = Some(randomDeadline(false)),
//            functionOutput =
//              eitherOne(
//                SegmentFunctionOutput.Expire(randomDeadline(false)),
//                SegmentFunctionOutput.Update(genStringSliceOptional(), randomDeadlineOption(false))
//              )
//          )
//
//        val put =
//          randomPutKeyValue(1, deadline = randomDeadlineOption(false))
//
//        val expected = PendingApplyMerger(pendingApply, put).runRandomIO.get
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning pendingApply
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning put
//
//        Get(1, ThreadReadState.random) shouldBe expected
//      }
//    }
//
//    "range's fromValue is put" in {
//      runThis(10.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        val fromValue = Value.put(Slice.Null, randomDeadlineOption(false))
//
//        val range = randomRangeKeyValue(1, 10, fromValue)
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning range
//
//        Get(1, ThreadReadState.random) shouldBe fromValue.toMemory(1)
//      }
//    }
//
//    "range's fromValue or/and rangeValue is a function that does not removes or expires" in {
//      runThis(30.times) {
//        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)
//
//        implicit val getFromCurrentLevel = mock[CurrentGetter]
//        implicit val getFromNextLevel = mock[NextGetter]
//
//        val functionValue =
//          randomKeyValueFunctionKeyValue(
//            key = 1,
//            output =
//              eitherOne(
//                SegmentFunctionOutput.Expire(randomDeadline(false)),
//                SegmentFunctionOutput.Update(genStringSliceOptional(), randomDeadlineOption(false))
//              )
//          )
//
//        val range = randomRangeKeyValue(1, 10, eitherOne(Value.FromValue.Null, functionValue.toRangeValue().runRandomIO.get), functionValue.toRangeValue().runRandomIO.get)
//        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(false))
//
//        val expected = FixedMerger(functionValue, put).runRandomIO.get
//
//        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning range
//        //next level can return anything it will be removed.
//        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning put
//
//        Get(1, ThreadReadState.random) shouldBe expected
//      }
//    }
//  }
//}
